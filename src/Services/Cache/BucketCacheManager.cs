using Agent.Services.Storage;
using Agent.Utils.Globals;

namespace Agent.Services.Cache;

/// <summary>
/// Manages the L1 (RAM) bucket cache with LRU eviction.
///
/// L1 (RAM): Globals._NODE.Buckets — bounded by MaxBytes, LRU eviction.
/// L2 (RocksDB on PVC SSD): All buckets always persisted — unbounded.
///
/// Hot buckets stay in L1 naturally (search/store touches update LRU timestamp).
/// Cold buckets get evicted to save RAM and are loaded on-demand from L2.
///
/// Eviction runs on a periodic timer AND inline during store when the budget
/// is exceeded — preventing unbounded growth between timer ticks.
/// </summary>
public static class BucketCacheManager
{
    private static RocksDbBucketStorage? _bucketStorage;
    private static Timer? _evictionTimer;

    /// <summary>Max bytes for L1 bucket cache.</summary>
    public static long MaxBytes { get; private set; } = 12L * 1024 * 1024 * 1024;

    /// <summary>Eviction interval in seconds.</summary>
    public static int EvictionIntervalSec { get; private set; } = 10;

    private static volatile bool _initialized = false;

    // Throttle inline eviction: don't run more often than every 500ms
    private static long _lastInlineEvictionTicks = 0;
    private static readonly long InlineEvictionCooldownTicks = TimeSpan.FromMilliseconds(500).Ticks;

    // Track approximate usage without scanning all buckets every time
    // Updated by eviction and inline checks. Not perfectly accurate but prevents OOM.
    private static long _approximateUsageBytes = 0;

    /// <summary>
    /// Initialize the cache manager with a reference to the bucket storage (for L2 loads).
    /// Must be called once at startup before any search/store operations.
    /// </summary>
    public static void Initialize(RocksDbBucketStorage bucketStorage, long maxBytes, int evictionIntervalSec)
    {
        _bucketStorage = bucketStorage;
        MaxBytes = maxBytes;
        EvictionIntervalSec = evictionIntervalSec;

        _evictionTimer = new Timer(
            EvictIfNeeded,
            null,
            TimeSpan.FromSeconds(evictionIntervalSec),
            TimeSpan.FromSeconds(evictionIntervalSec));

        _initialized = true;
        Console.WriteLine($"[BucketCache] Initialized: maxBytes={maxBytes / (1024 * 1024)}MB, evictionInterval={evictionIntervalSec}s");
    }

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

        return cached;
    }

    /// <summary>
    /// Get a bucket from L1, or load from L2 if not cached.
    /// Used by the store path to ensure a cold bucket's existing vectors
    /// are present before adding a new vector.
    /// Also triggers inline eviction if the cache is over budget.
    /// </summary>
    public static M_Bucket GetOrLoad(string bucketName)
    {
        // Inline budget check — runs at most every 500ms to prevent overhead
        TryInlineEviction();

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
    /// Triggers inline eviction if needed.
    /// </summary>
    public static void NotifyBucketGrew(long additionalBytes)
    {
        Interlocked.Add(ref _approximateUsageBytes, additionalBytes);
        // If we're significantly over budget, evict immediately
        if (Interlocked.Read(ref _approximateUsageBytes) > MaxBytes * 1.1)
        {
            TryInlineEviction();
        }
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

    /// <summary>
    /// Inline eviction: runs during store/load when the cache is over budget.
    /// Throttled to run at most every 500ms to prevent contention.
    /// </summary>
    private static void TryInlineEviction()
    {
        // Quick check: is approximate usage over budget?
        if (Interlocked.Read(ref _approximateUsageBytes) <= MaxBytes)
            return;

        // Throttle: don't run more often than every 500ms
        long nowTicks = DateTime.UtcNow.Ticks;
        long lastTicks = Interlocked.Read(ref _lastInlineEvictionTicks);
        if (nowTicks - lastTicks < InlineEvictionCooldownTicks)
            return;

        // CAS to claim this eviction run
        if (Interlocked.CompareExchange(ref _lastInlineEvictionTicks, nowTicks, lastTicks) != lastTicks)
            return; // Another thread is running eviction

        // Do a real scan + evict (same logic as timer callback)
        DoEviction("inline");
    }

    /// <summary>
    /// Timer callback: evict coldest buckets if over the RAM limit.
    /// </summary>
    private static void EvictIfNeeded(object? state)
    {
        DoEviction("timer");
    }

    /// <summary>
    /// Core eviction logic. Evicts LRU buckets until under MaxBytes.
    /// Uses a partial sort (select cheapest 20% by LRU time) for efficiency
    /// instead of sorting all buckets.
    /// </summary>
    private static void DoEviction(string trigger)
    {
        try
        {
            long currentUsage = EstimateTotalBytes();
            // Update the approximate tracking with the real value
            Interlocked.Exchange(ref _approximateUsageBytes, currentUsage);

            if (currentUsage <= MaxBytes) return;

            long toFree = currentUsage - MaxBytes;
            // Free an extra 10% buffer to avoid thrashing
            toFree = (long)(toFree * 1.1);

            int totalBuckets = Globals._NODE.Buckets.Count;
            if (totalBuckets == 0) return;

            // For large bucket counts, use a more efficient approach:
            // Sample up to 10,000 candidates, sort only those, evict from the coldest.
            // This avoids sorting millions of entries.
            int sampleSize = Math.Min(totalBuckets, 10_000);
            var candidates = new List<(string Key, long LastAccess, long MemBytes)>(sampleSize);

            if (totalBuckets <= 10_000)
            {
                // Small enough to scan everything
                foreach (var kv in Globals._NODE.Buckets)
                    candidates.Add((kv.Key, kv.Value.LastAccessedTicks, kv.Value.EstimatedMemoryBytes));
            }
            else
            {
                // Sample: take every Nth bucket to get ~10,000 candidates
                int step = totalBuckets / sampleSize;
                int idx = 0;
                foreach (var kv in Globals._NODE.Buckets)
                {
                    if (idx % step == 0)
                        candidates.Add((kv.Key, kv.Value.LastAccessedTicks, kv.Value.EstimatedMemoryBytes));
                    idx++;
                    if (candidates.Count >= sampleSize) break;
                }
            }

            candidates.Sort((a, b) => a.LastAccess.CompareTo(b.LastAccess));

            long freed = 0;
            int evictedCount = 0;
            foreach (var (key, _, memBytes) in candidates)
            {
                if (freed >= toFree) break;
                if (Globals._NODE.Buckets.TryRemove(key, out _))
                {
                    freed += memBytes;
                    evictedCount++;
                }
            }

            if (evictedCount > 0)
            {
                long newUsage = currentUsage - freed;
                Interlocked.Exchange(ref _approximateUsageBytes, newUsage);
                Console.WriteLine(
                    $"[BucketCache] [{trigger}] Evicted {evictedCount} cold buckets, freed ~{freed / (1024 * 1024)}MB. " +
                    $"Usage: {newUsage / (1024 * 1024)}MB / {MaxBytes / (1024 * 1024)}MB, " +
                    $"Remaining: {Globals._NODE.Buckets.Count} buckets");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[BucketCache] [{trigger}] Eviction error: {ex.Message}");
        }
    }
}
