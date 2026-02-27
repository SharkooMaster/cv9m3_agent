using Agent.Services.Storage;
using Agent.Utils.Globals;

namespace Agent.Services.Cache;

/// <summary>
/// Manages the L1 (RAM) bucket cache with system-level memory monitoring.
///
/// L1 (RAM): Globals._NODE.Buckets — bounded by MaxBytes, LRU eviction.
/// L2 (RocksDB on PVC SSD): All buckets always persisted — unbounded, loaded on-demand.
///
/// Eviction model:
///   1. SYSTEM MEMORY GUARD (highest priority):
///      Reads /proc/meminfo every 2 seconds. If MemAvailable drops below 10% of MemTotal,
///      aggressively evicts bucket cache AND chunk cache until free RAM is restored.
///      This catches ALL memory usage — managed heap, RocksDB native, mmap, gRPC buffers.
///
///   2. Cache-level watermarks (normal operation):
///      - High-water (70%): background eviction starts.
///      - Low-water (50%): background eviction target.
///      - Hard ceiling (90%): inline blocking eviction.
///
/// The system memory guard NEVER stops. It runs every 2 seconds regardless of anything else.
/// </summary>
public static class BucketCacheManager
{
    private static RocksDbBucketStorage? _bucketStorage;

    /// <summary>Max bytes for L1 bucket cache (100% budget).</summary>
    public static long MaxBytes { get; private set; } = 12L * 1024 * 1024 * 1024;

    public static double HighWaterPct { get; private set; } = 0.70;
    public static double LowWaterPct { get; private set; } = 0.50;
    public static double HardCeilingPct { get; private set; } = 0.90;

    private static long _highWaterBytes;
    private static long _lowWaterBytes;
    private static long _hardCeilingBytes;

    /// <summary>
    /// Minimum percentage of TOTAL NODE RAM that must remain free at all times.
    /// If MemAvailable drops below this, emergency eviction fires.
    /// </summary>
    private const double MinFreeRamPct = 0.10; // 10% of node RAM MUST stay free

    /// <summary>
    /// When emergency eviction fires, evict until free RAM reaches this percentage.
    /// Provides headroom so we don't thrash between evict/fill cycles.
    /// </summary>
    private const double TargetFreeRamPct = 0.15; // Evict until 15% is free

    private static volatile bool _initialized = false;
    private static long _approximateUsageBytes = 0;

    // ── Background evictor thread ──
    private static Thread? _evictorThread;
    private static readonly ManualResetEventSlim _evictSignal = new(false);
    private static volatile bool _shutdown = false;

    private const int EvictBatchSize = 500; // Larger batches for faster eviction

    // ── Callback to evict chunk cache (set by Program.cs) ──
    private static Action? _forceEvictChunkCache;

    /// <summary>
    /// Register a callback that the system memory guard can call to force-evict
    /// the chunk cache when node memory is critically low.
    /// </summary>
    public static void RegisterChunkCacheEvictor(Action evictor)
    {
        _forceEvictChunkCache = evictor;
    }

    public static void Initialize(
        RocksDbBucketStorage bucketStorage,
        long maxBytes,
        int evictionIntervalSec,
        long totalAvailableMemory = 0,
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

        _shutdown = false;
        _evictorThread = new Thread(BackgroundEvictorLoop)
        {
            IsBackground = true,
            Name = "BucketCache-Evictor",
            Priority = ThreadPriority.AboveNormal // Higher priority — this thread protects against OOM
        };
        _evictorThread.Start();

        _initialized = true;

        var (memTotal, memAvailable) = ReadSystemMemory();
        Console.WriteLine(
            $"[BucketCache] Initialized: budget={maxBytes / (1024 * 1024)}MB, " +
            $"highWater={highWaterPct:P0}({_highWaterBytes / (1024 * 1024)}MB), " +
            $"lowWater={lowWaterPct:P0}({_lowWaterBytes / (1024 * 1024)}MB), " +
            $"hardCeiling={hardCeilingPct:P0}({_hardCeilingBytes / (1024 * 1024)}MB), " +
            $"sysMemTotal={memTotal / (1024 * 1024)}MB, sysMemFree={memAvailable / (1024 * 1024)}MB, " +
            $"minFreeGuard={MinFreeRamPct:P0}({memTotal * MinFreeRamPct / (1024 * 1024):F0}MB)");
    }

    public static void Shutdown()
    {
        _shutdown = true;
        _evictSignal.Set();
        _evictorThread?.Join(TimeSpan.FromSeconds(5));
    }

    // ═══════════════════════════════════════════════════════════════════
    //  SYSTEM MEMORY READING — /proc/meminfo
    // ═══════════════════════════════════════════════════════════════════

    /// <summary>
    /// Reads MemTotal and MemAvailable from /proc/meminfo.
    /// These are REAL kernel values — they include ALL memory usage on the node:
    /// managed .NET heap, RocksDB native allocations, mmap'd files, kernel caches, etc.
    /// Falls back to GC info if /proc/meminfo is not available (non-Linux).
    /// </summary>
    private static (long MemTotal, long MemAvailable) ReadSystemMemory()
    {
        try
        {
            if (File.Exists("/proc/meminfo"))
            {
                long memTotal = 0, memAvailable = 0;
                foreach (var line in File.ReadLines("/proc/meminfo"))
                {
                    if (line.StartsWith("MemTotal:"))
                        memTotal = ParseMemInfoLine(line);
                    else if (line.StartsWith("MemAvailable:"))
                        memAvailable = ParseMemInfoLine(line);

                    if (memTotal > 0 && memAvailable > 0)
                        break;
                }
                if (memTotal > 0)
                    return (memTotal, memAvailable);
            }
        }
        catch { /* fall through to GC fallback */ }

        // Fallback for non-Linux (dev machines, etc.)
        var gcInfo = GC.GetGCMemoryInfo();
        long total = gcInfo.TotalAvailableMemoryBytes;
        long used = GC.GetTotalMemory(false);
        return (total, Math.Max(0, total - used));
    }

    /// <summary>
    /// Parses a /proc/meminfo line like "MemTotal:       32456789 kB" → bytes.
    /// </summary>
    private static long ParseMemInfoLine(string line)
    {
        // Format: "MemTotal:       32456789 kB"
        var parts = line.Split(':', 2);
        if (parts.Length < 2) return 0;
        var valuePart = parts[1].Trim();
        // Remove "kB" suffix
        if (valuePart.EndsWith("kB", StringComparison.OrdinalIgnoreCase))
            valuePart = valuePart[..^2].Trim();
        if (long.TryParse(valuePart, out long kb))
            return kb * 1024; // Convert kB to bytes
        return 0;
    }

    // ═══════════════════════════════════════════════════════════════════
    //  PUBLIC API
    // ═══════════════════════════════════════════════════════════════════

    public static M_Bucket? TryGet(string bucketName)
    {
        if (Globals._NODE.Buckets.TryGetValue(bucketName, out var bucket))
        {
            bucket.TouchAccess();
            return bucket;
        }
        return null;
    }

    public static M_Bucket? LoadAndCache(string bucketName)
    {
        if (Globals._NODE.Buckets.TryGetValue(bucketName, out var existing))
        {
            existing.TouchAccess();
            return existing;
        }

        if (_bucketStorage == null) return null;

        var vectors = _bucketStorage.LoadSingleBucketToMemory(bucketName);
        if (vectors == null || vectors.Count == 0)
            return null;

        var bucket = new M_Bucket(bucketName);
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

        var cached = Globals._NODE.Buckets.GetOrAdd(bucketName, bucket);
        cached.TouchAccess();

        // Only track if WE inserted (prevents double-counting on race)
        if (ReferenceEquals(cached, bucket))
        {
            Interlocked.Add(ref _approximateUsageBytes, cached.EstimatedMemoryBytes);
            CheckHighWater();
        }

        return cached;
    }

    public static M_Bucket GetOrLoad(string bucketName)
    {
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

    public static void NotifyBucketGrew(long additionalBytes)
    {
        Interlocked.Add(ref _approximateUsageBytes, additionalBytes);
        CheckHighWater();
    }

    public static long EstimateTotalBytes()
    {
        long total = 0;
        foreach (var kv in Globals._NODE.Buckets)
            total += kv.Value.EstimatedMemoryBytes;
        return total;
    }

    // ═══════════════════════════════════════════════════════════════════
    //  EVICTION LOGIC
    // ═══════════════════════════════════════════════════════════════════

    private static void CheckHighWater()
    {
        if (Interlocked.Read(ref _approximateUsageBytes) > _highWaterBytes)
            _evictSignal.Set();
    }

    private static void CheckHardCeiling()
    {
        if (Interlocked.Read(ref _approximateUsageBytes) <= _hardCeilingBytes)
            return;

        long currentUsage = EstimateTotalBytes();
        Interlocked.Exchange(ref _approximateUsageBytes, currentUsage);
        if (currentUsage <= _hardCeilingBytes) return;

        Console.WriteLine($"[BucketCache] ⚠️ HARD CEILING hit ({currentUsage / (1024 * 1024)}MB > {_hardCeilingBytes / (1024 * 1024)}MB). Inline eviction...");
        DoEvictionToTarget(_lowWaterBytes, "hardCeiling");
    }

    /// <summary>
    /// Background evictor thread. Runs every 2 seconds.
    /// Priority 1: System memory guard (reads /proc/meminfo).
    /// Priority 2: Cache-level watermark eviction.
    /// </summary>
    private static void BackgroundEvictorLoop()
    {
        Console.WriteLine("[BucketCache] Background evictor thread started (2s cycle, /proc/meminfo guard).");
        while (!_shutdown)
        {
            // Short 2-second cycle — we must react fast to memory pressure
            _evictSignal.Wait(TimeSpan.FromSeconds(2));
            _evictSignal.Reset();

            if (_shutdown) break;

            // ════════════════════════════════════════════════════════
            //  PRIORITY 1: SYSTEM MEMORY GUARD
            //  Reads REAL free memory from the kernel. If free < 10%
            //  of total, NUKE CACHES until free >= 15%.
            // ════════════════════════════════════════════════════════
            var (memTotal, memAvailable) = ReadSystemMemory();
            if (memTotal > 0)
            {
                double freePct = (double)memAvailable / memTotal;
                long minFreeBytes = (long)(memTotal * MinFreeRamPct);
                long targetFreeBytes = (long)(memTotal * TargetFreeRamPct);

                if (freePct < MinFreeRamPct)
                {
                    long deficit = targetFreeBytes - memAvailable; // how much we need to free

                    Console.WriteLine(
                        $"[BucketCache] 🚨 SYSTEM MEMORY CRITICAL: " +
                        $"free={memAvailable / (1024 * 1024)}MB ({freePct:P1}) < {MinFreeRamPct:P0} of {memTotal / (1024 * 1024)}MB. " +
                        $"Must free {deficit / (1024 * 1024)}MB");

                    // Step 1: Evict chunk cache ENTIRELY — it's the least important
                    try
                    {
                        _forceEvictChunkCache?.Invoke();
                        Console.WriteLine("[BucketCache] 🚨 Chunk cache cleared by system memory guard.");
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[BucketCache] Chunk cache eviction error: {ex.Message}");
                    }

                    // Step 2: Evict bucket cache aggressively
                    long cacheUsage = EstimateTotalBytes();
                    Interlocked.Exchange(ref _approximateUsageBytes, cacheUsage);

                    // Target: shed enough cache to free the deficit (+ margin)
                    long newCacheTarget = Math.Max(0, cacheUsage - deficit - (deficit / 2));
                    Console.WriteLine(
                        $"[BucketCache] 🚨 Evicting buckets: {cacheUsage / (1024 * 1024)}MB → {newCacheTarget / (1024 * 1024)}MB target");

                    DoEvictionToTarget(newCacheTarget, "SYSMEM");

                    // Step 3: Force GC to release managed memory back to OS
                    GC.Collect(2, GCCollectionMode.Aggressive, true, true);
                    GC.WaitForPendingFinalizers();
                    GC.Collect(2, GCCollectionMode.Aggressive, true, true);

                    // Re-read to verify
                    var (_, freeAfter) = ReadSystemMemory();
                    Console.WriteLine(
                        $"[BucketCache] 🚨 After emergency eviction: free={freeAfter / (1024 * 1024)}MB ({(double)freeAfter / memTotal:P1}), " +
                        $"bucketCache={Globals._NODE.Buckets.Count} buckets");

                    // If still not enough, loop will fire again in 2 seconds
                    continue;
                }

                // Warning zone: free < 15% — start normal eviction proactively
                if (freePct < TargetFreeRamPct)
                {
                    long cacheUsage = EstimateTotalBytes();
                    Interlocked.Exchange(ref _approximateUsageBytes, cacheUsage);
                    if (cacheUsage > _lowWaterBytes)
                    {
                        Console.WriteLine(
                            $"[BucketCache] ⚠️ System memory low: free={memAvailable / (1024 * 1024)}MB ({freePct:P1}). " +
                            $"Proactive eviction to low-water {_lowWaterBytes / (1024 * 1024)}MB");
                        DoEvictionToTarget(_lowWaterBytes, "proactive");

                        // Also trim chunk cache
                        try { _forceEvictChunkCache?.Invoke(); }
                        catch { }
                    }
                    continue;
                }
            }

            // ════════════════════════════════════════════════════════
            //  PRIORITY 2: NORMAL CACHE-LEVEL WATERMARK EVICTION
            // ════════════════════════════════════════════════════════
            long usage = EstimateTotalBytes();
            Interlocked.Exchange(ref _approximateUsageBytes, usage);

            if (usage <= _highWaterBytes) continue;

            Console.WriteLine(
                $"[BucketCache] [bg] Eviction: {usage / (1024 * 1024)}MB > high-water {_highWaterBytes / (1024 * 1024)}MB. " +
                $"Target: {_lowWaterBytes / (1024 * 1024)}MB. SysFree={memAvailable / (1024 * 1024)}MB");

            DoEvictionToTarget(_lowWaterBytes, "bg");
        }
        Console.WriteLine("[BucketCache] Background evictor thread stopped.");
    }

    /// <summary>
    /// Core eviction: evict LRU buckets until usage is at or below targetBytes.
    /// Uses large batches for fast eviction.
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

            // For emergency triggers, scan ALL buckets (no sampling)
            bool isEmergency = trigger == "SYSMEM" || trigger == "hardCeiling";
            int sampleSize = isEmergency ? Math.Min(totalBuckets, 100_000) : Math.Min(totalBuckets, 10_000);

            var candidates = new List<(string Key, long LastAccess, long MemBytes)>(sampleSize);

            if (totalBuckets <= sampleSize)
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

            candidates.Sort((a, b) => a.LastAccess.CompareTo(b.LastAccess));

            // Use larger batch for emergency eviction
            int batchSize = isEmergency ? 2000 : EvictBatchSize;
            int candidateIdx = 0;
            while (freed < toFree && candidateIdx < candidates.Count)
            {
                int batchEnd = Math.Min(candidateIdx + batchSize, candidates.Count);
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

                if (!isEmergency && freed < toFree)
                    Thread.Yield();
            }

            if (evictedCount > 0)
            {
                long newUsage = currentUsage - freed;
                Interlocked.Exchange(ref _approximateUsageBytes, Math.Max(0, newUsage));
                Console.WriteLine(
                    $"[BucketCache] [{trigger}] Evicted {evictedCount} buckets, freed ~{freed / (1024 * 1024)}MB. " +
                    $"Usage: {Math.Max(0, newUsage) / (1024 * 1024)}MB / {MaxBytes / (1024 * 1024)}MB, " +
                    $"Remaining: {Globals._NODE.Buckets.Count} buckets");
            }

            // If emergency and we didn't free enough, signal for immediate re-run
            if (isEmergency && freed < toFree && Globals._NODE.Buckets.Count > 0)
            {
                Console.WriteLine($"[BucketCache] [{trigger}] Need more eviction: freed {freed / (1024 * 1024)}MB of {toFree / (1024 * 1024)}MB needed.");
                _evictSignal.Set(); // wake up again immediately
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[BucketCache] [{trigger}] Eviction error: {ex.Message}");
        }
    }
}
