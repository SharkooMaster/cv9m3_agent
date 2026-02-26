using Agent.Services.Storage;
using Agent.Utils.Globals;

namespace Agent.Services.Cache;

/// <summary>
/// Manages the L1 (RAM) bucket cache with high-water/low-water background eviction.
///
/// L1 (RAM): Globals._NODE.Buckets — bounded by MaxBytes, LRU eviction.
/// L2 (RocksDB on PVC SSD): All buckets always persisted — unbounded.
///
/// Eviction model (non-blocking):
///   - High-water mark (default 90%): when cache usage exceeds this, a dedicated
///     background thread is signaled to start evicting.
///   - Low-water mark (default 75%): eviction continues until usage drops to this level,
///     creating ~15% headroom for burst writes.
///   - Hard ceiling (default 98%): emergency inline blocking eviction — should rarely fire.
///
/// Hot path (store/search) is NEVER blocked by eviction except at the hard ceiling.
/// The background thread evicts in small batches to avoid holding locks too long.
/// </summary>
public static class BucketCacheManager
{
    private static RocksDbBucketStorage? _bucketStorage;

    /// <summary>Max bytes for L1 bucket cache (100% budget).</summary>
    public static long MaxBytes { get; private set; } = 12L * 1024 * 1024 * 1024;

    /// <summary>High-water percentage (0.0–1.0). Background eviction starts when usage > MaxBytes * HighWaterPct.</summary>
    public static double HighWaterPct { get; private set; } = 0.70;

    /// <summary>Low-water percentage (0.0–1.0). Background eviction stops when usage <= MaxBytes * LowWaterPct.</summary>
    public static double LowWaterPct { get; private set; } = 0.50;

    /// <summary>Hard ceiling percentage (0.0–1.0). Inline blocking eviction fires when usage > MaxBytes * HardCeilingPct.</summary>
    public static double HardCeilingPct { get; private set; } = 0.90;

    // Derived byte thresholds (updated on Initialize)
    private static long _highWaterBytes;
    private static long _lowWaterBytes;
    private static long _hardCeilingBytes;

    private static volatile bool _initialized = false;

    // Track approximate usage without scanning all buckets every time
    private static long _approximateUsageBytes = 0;

    // ── Background evictor thread ──
    private static Thread? _evictorThread;
    private static readonly ManualResetEventSlim _evictSignal = new(false);
    private static volatile bool _shutdown = false;

    // Batching: evict this many entries per lock cycle to avoid starving the hot path
    private const int EvictBatchSize = 200;

    /// <summary>
    /// Initialize the cache manager with a reference to the bucket storage (for L2 loads).
    /// Must be called once at startup before any search/store operations.
    /// </summary>
    public static void Initialize(
        RocksDbBucketStorage bucketStorage,
        long maxBytes,
        int evictionIntervalSec, // kept for API compat but unused now
        double highWaterPct = 0.90,
        double lowWaterPct = 0.75,
        double hardCeilingPct = 0.98)
    {
        _bucketStorage = bucketStorage;
        MaxBytes = maxBytes;
        HighWaterPct = highWaterPct;
        LowWaterPct = lowWaterPct;
        HardCeilingPct = hardCeilingPct;

        _highWaterBytes = (long)(maxBytes * highWaterPct);
        _lowWaterBytes = (long)(maxBytes * lowWaterPct);
        _hardCeilingBytes = (long)(maxBytes * hardCeilingPct);

        // Start the dedicated background evictor thread
        _shutdown = false;
        _evictorThread = new Thread(BackgroundEvictorLoop)
        {
            IsBackground = true,
            Name = "BucketCache-Evictor",
            Priority = ThreadPriority.BelowNormal
        };
        _evictorThread.Start();

        _initialized = true;
        Console.WriteLine(
            $"[BucketCache] Initialized: budget={maxBytes / (1024 * 1024)}MB, " +
            $"highWater={highWaterPct:P0}({_highWaterBytes / (1024 * 1024)}MB), " +
            $"lowWater={lowWaterPct:P0}({_lowWaterBytes / (1024 * 1024)}MB), " +
            $"hardCeiling={hardCeilingPct:P0}({_hardCeilingBytes / (1024 * 1024)}MB)");
    }

    /// <summary>
    /// Call on application shutdown to stop the background evictor cleanly.
    /// </summary>
    public static void Shutdown()
    {
        _shutdown = true;
        _evictSignal.Set(); // wake up the thread so it exits
        _evictorThread?.Join(TimeSpan.FromSeconds(5));
    }

    // ═══════════════════════════════════════════════════════════════════
    //  PUBLIC API — used by search/store paths
    // ═══════════════════════════════════════════════════════════════════

    /// <summary>
    /// Try to get a bucket from L1 (RAM). Returns null on cache miss.
    /// Updates LRU timestamp on hit.
    /// </summary>
    public static M_Bucket? TryGet(string bucketName)
    {
        if (Globals._NODE.Buckets.TryGetValue(bucketName, out var bucket))
        {
            bucket.TouchAccess();
            return bucket;
        }
        return null;
    }

    /// <summary>
    /// Load a bucket from L2 (RocksDB) into L1 (RAM).
    /// Returns the cached bucket, or null if the bucket doesn't exist in L2.
    /// Thread-safe: uses GetOrAdd to prevent duplicate loads.
    /// Fully synchronous — safe to call from Parallel.For/search hot paths.
    /// </summary>
    public static M_Bucket? LoadAndCache(string bucketName)
    {
        // Fast path: already in L1 (another thread loaded it)
        if (Globals._NODE.Buckets.TryGetValue(bucketName, out var existing))
        {
            existing.TouchAccess();
            return existing;
        }

        if (_bucketStorage == null) return null;

        // Load from L2 (RocksDB) — fully synchronous, no async overhead
        var vectors = _bucketStorage.LoadSingleBucketToMemory(bucketName);
        if (vectors == null || vectors.Count == 0)
            return null;

        // Build the bucket
        var bucket = new M_Bucket(bucketName);
        foreach (var (vector, storageGuid, bucketId, bucketIndex) in vectors)
        {
            bucket.AddData(new M_Data
            {
                vector = vector,
                storageGuid = storageGuid,
                id = bucketId,
                index = bucketIndex,
                chunk = null // Chunks loaded on-demand from chunk cache / RocksDB
            });
        }
        bucket.TouchAccess();

        // Add to L1 — GetOrAdd ensures only one copy if another thread raced us
        var cached = Globals._NODE.Buckets.GetOrAdd(bucketName, bucket);
        cached.TouchAccess();

        // Track approximate usage increase
        Interlocked.Add(ref _approximateUsageBytes, cached.EstimatedMemoryBytes);

        // Signal background evictor if we crossed the high-water mark
        CheckHighWater();

        return cached;
    }

    /// <summary>
    /// Get a bucket from L1, or load from L2 if not cached.
    /// Used by the store path to ensure a cold bucket's existing vectors
    /// are present before adding a new vector.
    /// Also triggers background eviction if the cache is over high-water,
    /// or emergency inline eviction at the hard ceiling.
    /// </summary>
    public static M_Bucket GetOrLoad(string bucketName)
    {
        // Hard ceiling check — emergency inline eviction (should be very rare)
        CheckHardCeiling();

        return Globals._NODE.Buckets.GetOrAdd(bucketName, key =>
        {
            if (_bucketStorage != null)
            {
                var vectors = _bucketStorage.LoadSingleBucketToMemory(key);
                if (vectors != null && vectors.Count > 0)
                {
                    var bucket = new M_Bucket(key);
                    foreach (var (vector, storageGuid, bucketId, bucketIndex) in vectors)
                    {
                        bucket.AddData(new M_Data
                        {
                            vector = vector,
                            storageGuid = storageGuid,
                            id = bucketId,
                            index = bucketIndex,
                            chunk = null
                        });
                    }
                    bucket.TouchAccess();
                    Interlocked.Add(ref _approximateUsageBytes, bucket.EstimatedMemoryBytes);
                    CheckHighWater();
                    return bucket;
                }
            }
            var newBucket = new M_Bucket(key);
            Interlocked.Add(ref _approximateUsageBytes, newBucket.EstimatedMemoryBytes);
            return newBucket;
        });
    }

    /// <summary>
    /// Notify the cache that a bucket grew (e.g. new vector added during store).
    /// Signals background eviction if over high-water.
    /// </summary>
    public static void NotifyBucketGrew(long additionalBytes)
    {
        Interlocked.Add(ref _approximateUsageBytes, additionalBytes);
        CheckHighWater();
    }

    /// <summary>
    /// Estimate total RAM used by all L1 cached buckets.
    /// </summary>
    public static long EstimateTotalBytes()
    {
        long total = 0;
        foreach (var kv in Globals._NODE.Buckets)
            total += kv.Value.EstimatedMemoryBytes;
        return total;
    }

    // ═══════════════════════════════════════════════════════════════════
    //  INTERNAL — eviction logic
    // ═══════════════════════════════════════════════════════════════════

    /// <summary>
    /// Signal the background evictor if approximate usage exceeds the high-water mark.
    /// Non-blocking — just sets a flag.
    /// </summary>
    private static void CheckHighWater()
    {
        if (Interlocked.Read(ref _approximateUsageBytes) > _highWaterBytes)
            _evictSignal.Set();
    }

    /// <summary>
    /// Emergency inline eviction at the hard ceiling (98%).
    /// Blocks the calling thread until usage is back under the high-water mark.
    /// This should almost never fire — it's a safety valve.
    /// </summary>
    private static void CheckHardCeiling()
    {
        if (Interlocked.Read(ref _approximateUsageBytes) <= _hardCeilingBytes)
            return;

        // We're above 98% — do an immediate synchronous eviction to high-water
        long currentUsage = EstimateTotalBytes();
        Interlocked.Exchange(ref _approximateUsageBytes, currentUsage);
        if (currentUsage <= _hardCeilingBytes) return;

        Console.WriteLine($"[BucketCache] ⚠️ HARD CEILING hit ({currentUsage / (1024 * 1024)}MB > {_hardCeilingBytes / (1024 * 1024)}MB). Inline eviction to low-water...");
        DoEvictionToTarget(_lowWaterBytes, "hardCeiling");
    }

    /// <summary>
    /// Dedicated background evictor thread.
    /// Waits for a signal (ManualResetEventSlim), then evicts down to the low-water mark
    /// in small batches so it doesn't hold any lock for too long.
    /// </summary>
    private static void BackgroundEvictorLoop()
    {
        Console.WriteLine("[BucketCache] Background evictor thread started.");
        while (!_shutdown)
        {
            // Wait for signal (or periodic wakeup every 5s to re-check)
            _evictSignal.Wait(TimeSpan.FromSeconds(5));
            _evictSignal.Reset();

            if (_shutdown) break;

            // Re-check with accurate usage
            long currentUsage = EstimateTotalBytes();
            Interlocked.Exchange(ref _approximateUsageBytes, currentUsage);

            if (currentUsage <= _highWaterBytes) continue;

            Console.WriteLine(
                $"[BucketCache] [bg] Eviction triggered: {currentUsage / (1024 * 1024)}MB > high-water {_highWaterBytes / (1024 * 1024)}MB. " +
                $"Target: {_lowWaterBytes / (1024 * 1024)}MB (low-water)");

            DoEvictionToTarget(_lowWaterBytes, "bg");
        }
        Console.WriteLine("[BucketCache] Background evictor thread stopped.");
    }

    /// <summary>
    /// Core eviction: evict LRU buckets until usage is at or below <paramref name="targetBytes"/>.
    /// Processes in batches of <see cref="EvictBatchSize"/> to avoid starving the hot path.
    /// </summary>
    private static void DoEvictionToTarget(long targetBytes, string trigger)
    {
        try
        {
            long currentUsage = Interlocked.Read(ref _approximateUsageBytes);
            if (currentUsage <= targetBytes) return;

            long toFree = currentUsage - targetBytes;
            long freed = 0;
            int evictedCount = 0;
            int totalBuckets = Globals._NODE.Buckets.Count;
            if (totalBuckets == 0) return;

            // Collect eviction candidates — sample if there are too many buckets
            int sampleSize = Math.Min(totalBuckets, 10_000);
            var candidates = new List<(string Key, long LastAccess, long MemBytes)>(sampleSize);

            if (totalBuckets <= 10_000)
            {
                foreach (var kv in Globals._NODE.Buckets)
                    candidates.Add((kv.Key, kv.Value.LastAccessedTicks, kv.Value.EstimatedMemoryBytes));
            }
            else
            {
                int step = Math.Max(1, totalBuckets / sampleSize);
                int idx = 0;
                foreach (var kv in Globals._NODE.Buckets)
                {
                    if (idx % step == 0)
                        candidates.Add((kv.Key, kv.Value.LastAccessedTicks, kv.Value.EstimatedMemoryBytes));
                    idx++;
                    if (candidates.Count >= sampleSize) break;
                }
            }

            // Sort by LRU (oldest first)
            candidates.Sort((a, b) => a.LastAccess.CompareTo(b.LastAccess));

            // Evict in batches, yielding between batches to let the hot path run
            int candidateIdx = 0;
            while (freed < toFree && candidateIdx < candidates.Count)
            {
                int batchEnd = Math.Min(candidateIdx + EvictBatchSize, candidates.Count);
                for (int i = candidateIdx; i < batchEnd && freed < toFree; i++)
                {
                    var (key, _, memBytes) = candidates[i];
                    if (Globals._NODE.Buckets.TryRemove(key, out _))
                    {
                        freed += memBytes;
                        evictedCount++;
                    }
                }
                candidateIdx = batchEnd;

                // Yield to let hot-path threads acquire any shared resources
                if (freed < toFree)
                    Thread.Yield();
            }

            if (evictedCount > 0)
            {
                long newUsage = currentUsage - freed;
                Interlocked.Exchange(ref _approximateUsageBytes, newUsage);
                Console.WriteLine(
                    $"[BucketCache] [{trigger}] Evicted {evictedCount} cold buckets, freed ~{freed / (1024 * 1024)}MB. " +
                    $"Usage: {newUsage / (1024 * 1024)}MB / {MaxBytes / (1024 * 1024)}MB ({newUsage * 100 / Math.Max(1, MaxBytes)}%), " +
                    $"Remaining: {Globals._NODE.Buckets.Count} buckets");
            }

            // If we couldn't free enough from sampled candidates and there are more buckets,
            // re-run (background thread will be signaled again on next high-water check)
            if (freed < toFree && totalBuckets > 10_000)
            {
                Console.WriteLine($"[BucketCache] [{trigger}] Sampled eviction freed {freed / (1024 * 1024)}MB but needed {toFree / (1024 * 1024)}MB. Will re-check on next cycle.");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[BucketCache] [{trigger}] Eviction error: {ex.Message}");
        }
    }
}
