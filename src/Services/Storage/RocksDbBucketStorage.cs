using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Numerics;
using System.Runtime.InteropServices;
using System.Text;
using RocksDbSharp;

namespace Agent.Services.Storage;

/// <summary>
/// Stores bucket metadata and vectors in RocksDB instead of Postgres.
/// Key format: "bucket:{bucketName}" → JSON serialized list of vectors
/// </summary>
public sealed class RocksDbBucketStorage : IDisposable
{
    private readonly RocksDb _rocksDb;
    private readonly RocksDbWriteBatcher _writeBatcher;
    private readonly string _bucketDbPath;

    // Striped locks: 1024 lock objects instead of one per bucket.
    // Two buckets may share a stripe (brief contention, not correctness issue).
    // Eliminates the ConcurrentDictionary<string, object> that held 10M+ string keys.
    private static readonly object[] _bucketStripes = new object[1024];
    static RocksDbBucketStorage()
    {
        for (int i = 0; i < _bucketStripes.Length; i++)
            _bucketStripes[i] = new object();
    }
    private static object GetBucketLock(ulong bucketId) =>
        _bucketStripes[bucketId % (ulong)_bucketStripes.Length];

    // ── BOUNDED counter cache (replaces unbounded ConcurrentDictionary) ──
    // The previous implementation held one ulong→ulong entry per known bucket. At our
    // chunk size (1 KB) and corpus sizes (10s of GB → 10s of millions of buckets) this
    // grew to 5–8 GB managed heap and caused agent OOMs around the 14 Gi cgroup limit.
    //
    // The cache now keeps a hard-bounded LRU (default ~1M entries / a few hundred MB)
    // with sync-write-on-eviction to RocksDB's `bnext:{id}` key. Cold buckets fall back
    // to disk on the next access; the bloom-filtered point lookup is ~1–5 µs warm and
    // ~100 µs on a cache miss. Critically: the memory footprint no longer scales with
    // the number of distinct buckets ever ingested — only with the active working set.
    //
    // Concurrency invariant unchanged: per-bucket stripe locks serialise StoreVector
    // for the same bucketId. The shard locks inside the cache serialise concurrent
    // operations across different buckets that hash into the same shard.
    private readonly BoundedCounterCache<ulong> _bucketCounters;

    // Dedup is served directly from RocksDB via the bsg: key prefix.
    // Previously used an unbounded ConcurrentDictionary that grew to 7+ GB
    // during ingestion of large files (100M chunks × 144 bytes/entry = OOM).
    // Direct RocksDB Get() with bloom filters answers "not found" in ~1-5μs.

    // New append-friendly schema:
    // bn:{bucketName}              -> ulong bucketId
    // bi:{bucketId}                -> bucketName (utf8)
    // bnext:{bucketId}             -> ulong next vector index
    // bsg:{bucketId}:{storageGuid} -> ulong existing vector index (dedup)
    // bv:{bucketId}:{index}        -> binary record: [int dim][float * dim][string storageGuid][int chunkSize]
    //
    // Lane bucket schema (Level 2 sub-chunk index):
    // lv:{laneHash}:{index}        -> binary record: [ulong bucketId][ulong bucketIndex][byte lanePos][32 bytes storageGuid_raw]
    // lnext:{laneHash}             -> ulong next lane entry index
    private const string BucketNameToIdPrefix = "bn:";
    private const string BucketIdToNamePrefix = "bi:";
    private const string BucketNextPrefix = "bnext:";
    private const string BucketStorageGuidPrefix = "bsg:";
    private const string BucketVectorPrefix = "bv:";
    private const string LaneVectorPrefix = "lv:";
    private const string LaneNextPrefix = "lnext:";

    // Persistent storage stats (loaded at startup, snapshotted periodically).
    private const string MetaTotalBucketsKey = "meta:total_buckets";
    private const string MetaTotalVectorsKey = "meta:total_vectors";
    private const int MetaSnapshotIntervalMs = 30_000;

    // Binary key format for bucket vectors (replaces string "bv:{id}:{idx}")
    // [0x01][8-byte BE bucketId][8-byte BE index] = 17 bytes total, 9-byte prefix
    private const byte BinaryVectorTag = 0x01;
    private const int BinaryKeyLen = 17;
    private const int BinaryPrefixLen = 9;
    private static readonly byte[] MigrationMarkerKey = Encoding.UTF8.GetBytes("__bv_binary_v1__");

    // Lane counters: bounded by lane hash space (≤ 65k entries, ~5 MB managed). Loaded
    // lazily on first touch from `lnext:{hash}` so we keep the no-cold-start property.
    private static readonly ConcurrentDictionary<ushort, ulong> _laneNextIndex = new();
    private static readonly object _laneInitLock = new();
    private bool _laneCountersLoadedFromDisk;

    // In-memory stats counters; persisted under the `meta:` prefix.
    private long _totalBuckets;
    private long _totalVectors;
    private long _totalLaneEntries;
    private readonly Timer _statsSnapshotTimer;

    public RocksDbBucketStorage(string basePath, long blockCacheSizeMb = 128)
    {
        _bucketDbPath = Path.Combine(basePath, "buckets");
        Directory.CreateDirectory(_bucketDbPath);

        // ── RocksDB tuning for fast bucket lookups ──
        // Without this, every L2 read does unindexed SST block scans.
        // Bloom filter: skip SSTs that definitely don't have the key (~10 bits/key, <1% FPR)
        // Block cache: hot SST blocks stay in memory → subsequent reads are RAM-speed
        // Larger blocks: one read fetches more vectors during prefix scan
        var tableOpts = new BlockBasedTableOptions()
            .SetFilterPolicy(BloomFilterPolicy.Create(10, false))           // 10 bits/key bloom filter
            .SetBlockCache(RocksDbSharp.Cache.CreateLru((ulong)(blockCacheSizeMb * 1024 * 1024)))  // LRU block cache
            .SetBlockSize(16 * 1024)                                        // 16KB blocks (vs 4KB default)
            .SetCacheIndexAndFilterBlocks(true)                             // Keep index+bloom in block cache
            .SetPinL0FilterAndIndexBlocksInCache(true)                      // Pin L0 — fastest path stays hot
            .SetWholeKeyFiltering(false)                                    // Prefix bloom for Seek(), not whole-key
            .SetFormatVersion(4);                                           // Latest SST format

        var cfOpts = new ColumnFamilyOptions()
            .SetBlockBasedTableFactory(tableOpts)
            .SetPrefixExtractor(SliceTransform.CreateFixedPrefix(BinaryPrefixLen)) // 9-byte prefix bloom
            .SetMemtablePrefixBloomSizeRatio(0.1)                           // Memtable bloom for recent writes
            .SetWriteBufferSize(64 * 1024 * 1024)                           // 64MB memtable (vs 4MB default)
            .SetMaxWriteBufferNumber(4)                                     // 4 memtables before stall (more headroom)
            .SetLevel0FileNumCompactionTrigger(4)                           // Compact after 4 L0 files
            .SetLevel0SlowdownWritesTrigger(20)                             // Soft throttle at 20 L0 files (default)
            .SetLevel0StopWritesTrigger(48)                                 // Hard stall at 48 (vs default 36)
            .SetMaxBytesForLevelBase(512 * 1024 * 1024)                    // 512MB L1 (vs default 256MB) — reduces write amp
            .SetCompression(Compression.Lz4);

        int bgThreads = Math.Max(4, Environment.ProcessorCount / 2);
        var dbOpts = new DbOptions()
            .SetCreateIfMissing(true)
            .IncreaseParallelism(bgThreads)
            .SetMaxBackgroundCompactions(bgThreads)
            .SetMaxBackgroundFlushes(Math.Max(2, bgThreads / 2))
            .SetAdviseRandomOnOpen(false);

        var columnFamilies = new ColumnFamilies { { "default", cfOpts } };
        _rocksDb = RocksDb.Open(dbOpts, _bucketDbPath, columnFamilies);

        // Initialize write batcher (batches writes in background)
        // High-throughput: Larger batches for better performance
        _writeBatcher = new RocksDbWriteBatcher(_rocksDb, batchSize: 10_000, flushIntervalMs: 5_000);

        // ── Bounded counter cache for `bnext:{bucketId}` ──
        // Sized to comfortably exceed any plausible active working set while keeping
        // managed memory ~constant. Tunable via env var; default 1M entries × ~150 B ≈
        // 150–200 MB managed regardless of how many total buckets the agent has ingested.
        int counterCacheCapacity = ParseEnvInt("BUCKET_COUNTER_CACHE_CAPACITY", 1_000_000, 8_192);
        int counterCacheShards = ParseEnvInt("BUCKET_COUNTER_CACHE_SHARDS", 64, 1);
        _bucketCounters = new BoundedCounterCache<ulong>(
            totalCapacity: counterCacheCapacity,
            shardCount: counterCacheShards,
            diskRead: bucketId =>
            {
                var bytes = _rocksDb.Get(Encoding.UTF8.GetBytes($"{BucketNextPrefix}{bucketId}"));
                if (bytes == null || bytes.Length != sizeof(ulong)) return null;
                return BitConverter.ToUInt64(bytes, 0);
            },
            diskWriteSync: (bucketId, value) =>
            {
                _rocksDb.Put(
                    Encoding.UTF8.GetBytes($"{BucketNextPrefix}{bucketId}"),
                    BitConverter.GetBytes(value));
            },
            backgroundFlushIntervalMs: 5_000);

        // ── Stats counters (persistent under meta: prefix) ──
        _totalBuckets = ReadMetaCounter(MetaTotalBucketsKey);
        _totalVectors = ReadMetaCounter(MetaTotalVectorsKey);
        _statsSnapshotTimer = new Timer(_ => SnapshotStats(), null, MetaSnapshotIntervalMs, MetaSnapshotIntervalMs);
        Console.WriteLine($"[RocksDB Buckets] Stats loaded: totalBuckets={_totalBuckets:N0}, totalVectors={_totalVectors:N0} (snapshot every {MetaSnapshotIntervalMs / 1000}s)");

        // One-time migration: convert string bv: keys to binary format for prefix bloom
        MigrateToBinaryKeys();

        Console.WriteLine($"[RocksDB Buckets] Initialized at {_bucketDbPath} with prefix bloom + {blockCacheSizeMb}MB block cache + LZ4");
    }

    private static int ParseEnvInt(string name, int defaultValue, int min)
    {
        var raw = Environment.GetEnvironmentVariable(name);
        if (int.TryParse(raw, out var v) && v >= min) return v;
        return defaultValue;
    }

    private long ReadMetaCounter(string key)
    {
        var bytes = _rocksDb.Get(Encoding.UTF8.GetBytes(key));
        if (bytes == null || bytes.Length != sizeof(long)) return 0;
        return BitConverter.ToInt64(bytes, 0);
    }

    private void SnapshotStats()
    {
        try
        {
            _rocksDb.Put(Encoding.UTF8.GetBytes(MetaTotalBucketsKey), BitConverter.GetBytes(Interlocked.Read(ref _totalBuckets)));
            _rocksDb.Put(Encoding.UTF8.GetBytes(MetaTotalVectorsKey), BitConverter.GetBytes(Interlocked.Read(ref _totalVectors)));
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[RocksDB Buckets] Stats snapshot failed: {ex.GetType().Name}: {ex.Message}");
        }
    }

    /// <summary>
    /// Flush pending bucket metadata writes to RocksDB.
    /// Called by RocksDbStorageService.FlushPendingWrites().
    /// </summary>
    public void FlushWrites()
    {
        _writeBatcher?.Flush();
    }

    public void Dispose()
    {
        try { _statsSnapshotTimer?.Dispose(); } catch { }
        try { SnapshotStats(); } catch { }
        try { _bucketCounters?.Dispose(); } catch { }   // sync-flushes dirty counters
        _writeBatcher?.Flush();                         // flush pending vector records
        _writeBatcher?.Dispose();
        _rocksDb?.Dispose();
    }

    /// <summary>
    /// Lazily prime the lane-counter dictionary from `lnext:{hash}` keys on first lane
    /// write. Lane hash space is bounded (≤65k entries, ~5 MB), so loading the whole map
    /// up front is fine — but only when lanes are actually used. Bucket counters are NOT
    /// pre-loaded; they live in BoundedCounterCache and are pulled in on demand.
    /// </summary>
    private void EnsureLaneCountersLoaded()
    {
        if (_laneCountersLoadedFromDisk) return;

        lock (_laneInitLock)
        {
            if (_laneCountersLoadedFromDisk) return;

            var readOpts = new ReadOptions().SetTotalOrderSeek(true);
            using var iterator = _rocksDb.NewIterator(readOptions: readOpts);
            iterator.Seek(Encoding.UTF8.GetBytes(LaneNextPrefix));

            int loaded = 0;
            long entrySum = 0;
            while (iterator.Valid())
            {
                var keyBytes = iterator.Key();
                if (keyBytes == null || keyBytes.Length == 0) break;

                var key = Encoding.UTF8.GetString(keyBytes);
                if (!key.StartsWith(LaneNextPrefix, StringComparison.Ordinal)) break;

                var valueBytes = iterator.Value();
                if (valueBytes != null && valueBytes.Length == sizeof(ulong))
                {
                    var hashStr = key.AsSpan(LaneNextPrefix.Length);
                    if (ushort.TryParse(hashStr, out var laneHash))
                    {
                        var v = BitConverter.ToUInt64(valueBytes, 0);
                        _laneNextIndex[laneHash] = v;
                        entrySum += (long)v;
                        loaded++;
                    }
                }
                iterator.Next();
            }

            Interlocked.Exchange(ref _totalLaneEntries, entrySum);
            _laneCountersLoadedFromDisk = true;
            Console.WriteLine($"[RocksDB Buckets] Lazy-loaded {loaded} lane counters (totalEntries={entrySum:N0})");
        }
    }

    /// <summary>
    /// Bucket IDs are a 1:1 deterministic function of the bucket name (64-char bitstring),
    /// so we don't need an "exists?" check or a bn:/bi: registration step before storing.
    /// First-time-ever registration of a bucket happens inside StoreVector via the
    /// counter cache's onFirstFetch hook, so a never-touched bucket costs nothing.
    /// </summary>
    private static ulong DeriveBucketId(string bucketName) => BitstringToUlong(bucketName);

    /// <summary>
    /// Register a brand-new bucket: write the `bn:`/`bi:` mappings via the batcher.
    /// Called inside the counter cache's first-fetch hook, so it runs at most once per
    /// bucketId per agent instance.
    /// </summary>
    private void RegisterBucketMetadata(string bucketName, ulong bucketId)
    {
        _writeBatcher.Put(Encoding.UTF8.GetBytes($"{BucketNameToIdPrefix}{bucketName}"), BitConverter.GetBytes(bucketId));
        _writeBatcher.Put(Encoding.UTF8.GetBytes($"{BucketIdToNamePrefix}{bucketId}"), Encoding.UTF8.GetBytes(bucketName));
        Interlocked.Increment(ref _totalBuckets);
    }

    /// <summary>
    /// Convert a 64-char '0'/'1' bitstring into a ulong. 1:1 bijection — no collisions.
    /// Bit 0 of the string maps to bit 0 of the ulong, etc.
    /// </summary>
    internal static ulong BitstringToUlong(string bitstring) =>
        BitstringToUlong(bitstring.AsSpan());

    internal static ulong BitstringToUlong(ReadOnlySpan<char> bitstring)
    {
        ulong result = 0;
        int len = Math.Min(64, bitstring.Length);
        for (int i = 0; i < len; i++)
            if (bitstring[i] == '1') result |= (1UL << i);
        return result;
    }

    /// <summary>
    /// Convert a ulong back to a 64-char '0'/'1' bitstring. Inverse of BitstringToUlong.
    /// </summary>
    internal static string UlongToBitstring(ulong packed)
    {
        char[] chars = new char[64];
        for (int i = 0; i < 64; i++)
            chars[i] = (packed & (1UL << i)) != 0 ? '1' : '0';
        return new string(chars);
    }

    // ══════════════════════════════════════════════════════════════
    //  BINARY KEY HELPERS
    // ══════════════════════════════════════════════════════════════

    private static byte[] MakeBinaryVectorKey(ulong bucketId, ulong index)
    {
        var key = new byte[BinaryKeyLen];
        key[0] = BinaryVectorTag;
        BinaryPrimitives.WriteUInt64BigEndian(key.AsSpan(1), bucketId);
        BinaryPrimitives.WriteUInt64BigEndian(key.AsSpan(9), index);
        return key;
    }

    private static byte[] MakeBinaryVectorPrefix(ulong bucketId)
    {
        var key = new byte[BinaryPrefixLen];
        key[0] = BinaryVectorTag;
        BinaryPrimitives.WriteUInt64BigEndian(key.AsSpan(1), bucketId);
        return key;
    }

    private static bool TryParseBinaryVectorKey(byte[] key, out ulong bucketId, out ulong index)
    {
        bucketId = 0; index = 0;
        if (key == null || key.Length < BinaryKeyLen || key[0] != BinaryVectorTag)
            return false;
        bucketId = BinaryPrimitives.ReadUInt64BigEndian(key.AsSpan(1));
        index = BinaryPrimitives.ReadUInt64BigEndian(key.AsSpan(9));
        return true;
    }

    // ══════════════════════════════════════════════════════════════
    //  ONE-TIME MIGRATION: string bv: keys → binary keys
    // ══════════════════════════════════════════════════════════════

    private void MigrateToBinaryKeys()
    {
        var marker = _rocksDb.Get(MigrationMarkerKey);
        if (marker != null && marker.Length > 0)
            return; // Already migrated

        var sw = System.Diagnostics.Stopwatch.StartNew();
        var readOpts = new ReadOptions().SetTotalOrderSeek(true);
        var oldPrefix = Encoding.UTF8.GetBytes(BucketVectorPrefix);

        long migrated = 0;
        long batchCount = 0;
        const int BatchLimit = 50_000;

        using var iterator = _rocksDb.NewIterator(readOptions: readOpts);
        iterator.Seek(oldPrefix);

        var batch = new WriteBatch();
        while (iterator.Valid())
        {
            var keyBytes = iterator.Key();
            if (keyBytes == null || keyBytes.Length < oldPrefix.Length ||
                !keyBytes.AsSpan(0, oldPrefix.Length).SequenceEqual(oldPrefix))
                break;

            // Parse old string key: "bv:{bucketId}:{index}"
            var keySuffix = Encoding.UTF8.GetString(keyBytes, oldPrefix.Length, keyBytes.Length - oldPrefix.Length);
            var colonIdx = keySuffix.IndexOf(':');
            if (colonIdx >= 0 &&
                ulong.TryParse(keySuffix.AsSpan(0, colonIdx), out var bucketId) &&
                ulong.TryParse(keySuffix.AsSpan(colonIdx + 1), out var index))
            {
                var newKey = MakeBinaryVectorKey(bucketId, index);
                var val = iterator.Value();
                batch.Put(newKey, val);
                batch.Delete(keyBytes);
                migrated++;
            }

            iterator.Next();

            if (migrated % BatchLimit == 0 && migrated > 0)
            {
                _rocksDb.Write(batch);
                batch.Dispose();
                batch = new WriteBatch();
                batchCount++;
            }
        }

        // Final batch + migration marker
        batch.Put(MigrationMarkerKey, new byte[] { 1 });
        _rocksDb.Write(batch);
        batch.Dispose();

        sw.Stop();
        if (migrated > 0)
            Console.WriteLine($"[RocksDB Buckets] Migrated {migrated:N0} vector keys to binary format in {sw.ElapsedMilliseconds}ms ({batchCount + 1} batches)");
        else
            Console.WriteLine("[RocksDB Buckets] Binary key migration marker set (no old keys found)");
    }

    /// <summary>
    /// Store a vector in a bucket. Returns (bucketId, bucketIndex).
    /// Thread-safe: uses striped locking to allow concurrent writes to different buckets.
    /// </summary>
    public (ulong bucketId, ulong bucketIndex) StoreVector(string bucketName, float[] vector, string storageGuid, int chunkSize)
    {
        var bucketId = DeriveBucketId(bucketName);

        lock (GetBucketLock(bucketId))
        {
            // ── Dedup check: direct RocksDB lookup on bsg: key ──
            // Written synchronously (not batched) so it's immediately visible
            // to concurrent callers within the same bucket lock.
            var dedupKeyStr = $"{BucketStorageGuidPrefix}{bucketId}:{storageGuid}";
            var dedupKeyBytes = Encoding.UTF8.GetBytes(dedupKeyStr);
            var existingBytes = _rocksDb.Get(dedupKeyBytes);
            if (existingBytes != null && existingBytes.Length == sizeof(ulong))
            {
                ulong existingIndex = BitConverter.ToUInt64(existingBytes, 0);
                
                // ── GHOST RECORD HEALING ──
                // If the batcher failed previously, the dedup key might exist but the vector record
                // might be missing. If it's missing, rewrite it to the batcher.
                var vectorKeyBytes = MakeBinaryVectorKey(bucketId, existingIndex);
                var vecRecord = _rocksDb.Get(vectorKeyBytes);
                if (vecRecord == null || vecRecord.Length == 0)
                {
                    Console.WriteLine($"[RocksDbBucketStorage] Healing missing vector record for {bucketId}:{existingIndex}");
                    var recordBytes = SerializeVectorRecord(vector, storageGuid, chunkSize);
                    _writeBatcher.Put(vectorKeyBytes, recordBytes);
                }
                
                return (bucketId, existingIndex);
            }

            // Allocate the next index from the bounded counter cache. If the bucket has
            // never been seen on this agent (and isn't on disk), the cache returns 0 and
            // invokes RegisterBucketMetadata exactly once for the bn:/bi: writes.
            // The counter is the cache's authoritative value: the cache flushes it sync
            // to RocksDB on eviction or at the periodic snapshot interval, so subsequent
            // readers (cache miss → disk fallback) see the latest value.
            ulong bucketIndex = _bucketCounters.FetchAndIncrement(
                bucketId,
                onFirstFetch: () => RegisterBucketMetadata(bucketName, bucketId));

            // Write dedup entry directly to RocksDB (not batched) for immediate visibility
            _rocksDb.Put(dedupKeyBytes, BitConverter.GetBytes(bucketIndex));

            // Persist vector record via batcher (background). We deliberately do NOT
            // batch a `bnext:` write here — the cache is the sole writer of that key,
            // which prevents stale-overwrite races between the batcher and the cache's
            // sync flush on eviction.
            var recordBytes = SerializeVectorRecord(vector, storageGuid, chunkSize);
            var vectorKeyBytes = MakeBinaryVectorKey(bucketId, bucketIndex);
            _writeBatcher.Put(vectorKeyBytes, recordBytes);

            Interlocked.Increment(ref _totalVectors);

            return (bucketId, bucketIndex);
        }
    }

    /// <summary>
    /// Get all vectors for a list of bucket names.
    /// </summary>
    public List<(float[] vector, string storageGuid, ulong bucketId, ulong bucketIndex, string bucketName)> GetVectorsByBuckets(List<string> bucketNames)
    {
        var results = new List<(float[] vector, string storageGuid, ulong bucketId, ulong bucketIndex, string bucketName)>();
        var readOpts = new ReadOptions()
            .SetPrefixSameAsStart(true)
            .SetFillCache(true)
            .SetReadaheadSize((ulong)(256 * 1024));

        foreach (var bucketName in bucketNames)
        {
            var bucketId = BitstringToUlong(bucketName);
            if (bucketId == 0) continue;

            // No in-memory existence check: a bucket with no records returns zero rows
            // from the prefix iterator below (bloom-filtered, ~1–5 µs cold).
            var prefix = MakeBinaryVectorPrefix(bucketId);
            using var iterator = _rocksDb.NewIterator(readOptions: readOpts);
            iterator.Seek(prefix);

            while (iterator.Valid())
            {
                var keyBytes = iterator.Key();
                if (!TryParseBinaryVectorKey(keyBytes, out _, out var bucketIndex))
                {
                    iterator.Next();
                    continue;
                }

                var recordBytes = iterator.Value();
                if (recordBytes != null && recordBytes.Length > 0 && TryDeserializeVectorRecord(recordBytes, out var rec))
                    results.Add((rec.Vector, rec.StorageGuid, bucketId, bucketIndex, bucketName));

                iterator.Next();
            }
        }

        return results;
    }

    /// <summary>
    /// Get storage GUID by bucket ID and index.
    /// O(1) lookup using direct vector key.
    /// </summary>
    public string? GetStorageGuidByReference(ulong bucketId, ulong bucketIndex)
    {
        var key = MakeBinaryVectorKey(bucketId, bucketIndex);
        var recordBytes = _rocksDb.Get(key);
        if (recordBytes == null || recordBytes.Length == 0)
            return null;

        return TryDeserializeVectorRecord(recordBytes, out var rec) ? rec.StorageGuid : null;
    }

    /// <summary>
    /// Load a single bucket's vectors from RocksDB into memory.
    /// Used by BucketCacheManager for on-demand L2 → L1 promotion.
    /// Uses a prefix iterator (1 seek + sequential scan) instead of N random
    /// point-lookups. This is ~10x faster for large buckets because RocksDB
    /// prefetches SST blocks during iteration (cache-friendly sequential I/O).
    /// Fully synchronous — no async overhead for the hot path.
    /// </summary>
    public List<(float[] vector, string storageGuid, ulong bucketId, ulong bucketIndex, float normSquared)>? LoadSingleBucketToMemory(string bucketName)
    {
        var bucketId = BitstringToUlong(bucketName);
        if (bucketId == 0) return null;

        var prefix = MakeBinaryVectorPrefix(bucketId);
        var vectors = new List<(float[], string, ulong, ulong, float)>();
        var readOpts = new ReadOptions()
            .SetPrefixSameAsStart(true)
            .SetFillCache(true)
            .SetReadaheadSize((ulong)(256 * 1024));

        using var iterator = _rocksDb.NewIterator(readOptions: readOpts);
        iterator.Seek(prefix);

        while (iterator.Valid())
        {
            var keyBytes = iterator.Key();
            if (!TryParseBinaryVectorKey(keyBytes, out _, out var bucketIndex))
            {
                iterator.Next();
                continue;
            }

            var recordBytes = iterator.Value();
            if (recordBytes != null && recordBytes.Length > 0 && TryDeserializeVectorRecord(recordBytes, out var rec))
                vectors.Add((rec.Vector, rec.StorageGuid, bucketId, bucketIndex, rec.NormSquared));

            iterator.Next();
        }

        return vectors.Count > 0 ? vectors : null;
    }

    /// <summary>
    /// Load ALL bucket vectors from RocksDB into memory.
    /// Called once at startup to pre-warm Globals._NODE.Buckets so the hot path never touches disk.
    /// Uses a single full-range iterator scan — O(N) sequential I/O, much faster than
    /// per-bucket random reads especially with millions of vectors.
    /// </summary>
    public Dictionary<string, List<(float[] vector, string storageGuid, ulong bucketId, ulong bucketIndex, float normSquared)>> LoadAllBucketsToMemory()
    {
        var result = new Dictionary<string, List<(float[] vector, string storageGuid, ulong bucketId, ulong bucketIndex, float normSquared)>>();

        // Total-order seek: scan ALL binary vector keys (0x01 prefix)
        var readOpts = new ReadOptions().SetTotalOrderSeek(true);
        using var iterator = _rocksDb.NewIterator(readOptions: readOpts);
        iterator.Seek(new byte[] { BinaryVectorTag });

        while (iterator.Valid())
        {
            var keyBytes = iterator.Key();
            if (keyBytes == null || keyBytes.Length < BinaryKeyLen || keyBytes[0] != BinaryVectorTag)
                break;

            if (!TryParseBinaryVectorKey(keyBytes, out var bucketId, out var bucketIndex))
            {
                iterator.Next();
                continue;
            }

            var recordBytes = iterator.Value();
            if (recordBytes != null && recordBytes.Length > 0 && TryDeserializeVectorRecord(recordBytes, out var rec))
            {
                // The presence of a vector record IS the existence proof — no need for
                // a separate in-memory lookup (the previous check was a vestigial guard
                // from when the in-memory dict doubled as a "known buckets" set).
                var bucketName = UlongToBitstring(bucketId);
                if (!result.TryGetValue(bucketName, out var list))
                {
                    list = new List<(float[], string, ulong, ulong, float)>();
                    result[bucketName] = list;
                }
                list.Add((rec.Vector, rec.StorageGuid, bucketId, bucketIndex, rec.NormSquared));
            }

            iterator.Next();
        }

        return result;
    }

    /// <summary>
    /// Get fast O(1) bucket and vector counts from persistent in-memory counters.
    /// Counters are loaded from RocksDB at startup (via meta: keys), incremented in
    /// memory by StoreVector, and snapshotted back to RocksDB every 30 seconds and on
    /// graceful shutdown — so they survive restart without depending on iterating the
    /// (potentially huge) bucket-name space.
    /// </summary>
    public (long totalBuckets, long totalVectors) GetBucketAndVectorStats()
    {
        return (Interlocked.Read(ref _totalBuckets), Interlocked.Read(ref _totalVectors));
    }

    private static byte[] SerializeVectorRecord(float[] vector, string storageGuid, int chunkSize)
    {
        using var ms = new MemoryStream();
        using var writer = new BinaryWriter(ms, Encoding.UTF8, leaveOpen: true);
        writer.Write(vector.Length);
        for (int i = 0; i < vector.Length; i++)
            writer.Write(vector[i]);
        writer.Write(storageGuid ?? string.Empty);
        writer.Write(chunkSize);
        writer.Write(Agent.Utils.Misc.Misc.ComputeNormSquared(vector));
        writer.Flush();
        return ms.ToArray();
    }

    private static bool TryDeserializeVectorRecord(byte[] bytes, out VectorRecord rec)
    {
        rec = default;
        try
        {
            using var ms = new MemoryStream(bytes, writable: false);
            using var reader = new BinaryReader(ms, Encoding.UTF8, leaveOpen: true);
            var dim = reader.ReadInt32();
            if (dim <= 0 || dim > 4096)
                return false;

            var vector = new float[dim];
            for (int i = 0; i < dim; i++)
                vector[i] = reader.ReadSingle();

            var storageGuid = reader.ReadString();
            var chunkSize = reader.ReadInt32();

            if (string.IsNullOrWhiteSpace(storageGuid))
                return false;

            // Backward compat: old records don't have normSquared — compute on load
            float normSq;
            if (ms.Position < ms.Length - 3) // at least 4 bytes remain for the float
                normSq = reader.ReadSingle();
            else
                normSq = Agent.Utils.Misc.Misc.ComputeNormSquared(vector);

            rec = new VectorRecord(vector, storageGuid, chunkSize, normSq);
            return true;
        }
        catch
        {
            return false;
        }
    }

    private readonly record struct VectorRecord(float[] Vector, string StorageGuid, int ChunkSize, float NormSquared);

    // ══════════════════════════════════════════════════════════════
    //  DIRECT ROCKSDB SEARCH (zero M_Data allocation)
    // ══════════════════════════════════════════════════════════════

    /// <summary>
    /// Search a bucket directly from RocksDB without materializing M_Bucket/M_Data objects.
    /// Uses prefix bloom filter for O(1) SST skip, then iterates entries inline with SIMD
    /// cosine similarity. Only allocates byte[] per-entry (from iterator.Value()) and one
    /// string for the best match's storageGuid.
    /// Returns (bucketIndex, storageGuid, similarity) or (0, null, -1) if no match above threshold.
    /// </summary>
    public (ulong bucketIndex, string? storageGuid, float similarity) SearchBucketDirect(
        ulong bucketId, float[] queryVector, float queryNormSq, float threshold)
    {
        var prefix = MakeBinaryVectorPrefix(bucketId);
        var readOpts = new ReadOptions()
            .SetPrefixSameAsStart(true)
            .SetFillCache(true)
            .SetVerifyChecksums(false)
            .SetReadaheadSize((ulong)(256 * 1024));

        int vecLen = queryVector.Length;
        int vectorBytesLen = vecLen * sizeof(float);
        int minValueLen = 4 + vectorBytesLen; // dim + vector floats

        float bestSim = -1f;
        byte[]? bestKeyBytes = null;
        byte[]? bestValBytes = null;

        using var iter = _rocksDb.NewIterator(readOptions: readOpts);
        iter.Seek(prefix);

        while (iter.Valid())
        {
            var valBytes = iter.Value();
            if (valBytes == null || valBytes.Length < minValueLen)
            {
                iter.Next();
                continue;
            }

            // Inline deserialization: read dim, then cast vector bytes to Span<float>
            int dim = BitConverter.ToInt32(valBytes, 0);
            if (dim != vecLen || valBytes.Length < 4 + dim * sizeof(float))
            {
                iter.Next();
                continue;
            }

            var candidateSpan = MemoryMarshal.Cast<byte, float>(
                valBytes.AsSpan(4, dim * sizeof(float)));

            // Extract normSquared from end of record (after variable-length storageGuid)
            float normSq = ExtractNormSquared(valBytes, dim);

            float sim = CosineSimilarityInline(queryVector, queryNormSq, candidateSpan, normSq);
            if (sim >= threshold && sim > bestSim)
            {
                bestSim = sim;
                bestKeyBytes = iter.Key();
                bestValBytes = valBytes;
            }

            iter.Next();
        }

        if (bestKeyBytes == null || bestValBytes == null)
            return (0, null, -1f);

        ulong bestIndex = BinaryPrimitives.ReadUInt64BigEndian(bestKeyBytes.AsSpan(9));
        string? bestGuid = ExtractStorageGuid(bestValBytes, vecLen);
        return (bestIndex, bestGuid, bestSim);
    }

    /// <summary>
    /// Search multiple buckets directly from RocksDB. Collects up to maxCandidates vectors
    /// across all buckets and returns the single best match above threshold.
    /// Used by SearchVector.ProcessSingleQuery for cold (evicted) buckets.
    /// </summary>
    public (ulong bucketId, ulong bucketIndex, string? storageGuid, float similarity) SearchBucketsDirect(
        IReadOnlyList<ulong> bucketIds, float[] queryVector, float queryNormSq, float threshold, int maxCandidates = 4096)
    {
        if (bucketIds.Count == 0)
            return (0, 0, null, -1f);

        int vecLen = queryVector.Length;
        int vectorBytesLen = vecLen * sizeof(float);
        int minValueLen = 4 + vectorBytesLen;
        int perBucketBudget = Math.Max(64, maxCandidates / Math.Max(1, bucketIds.Count));

        int bucketCount = bucketIds.Count;
        var perBucket = new (float sim, ulong bucketId, byte[]? keyBytes, byte[]? valBytes)[bucketCount];

        int maxPar = Math.Min(4, bucketCount);
        Parallel.For(0, bucketCount, new ParallelOptions { MaxDegreeOfParallelism = maxPar }, b =>
        {
            ulong bucketId = bucketIds[b];
            var prefix = MakeBinaryVectorPrefix(bucketId);

            var readOpts = new ReadOptions()
                .SetPrefixSameAsStart(true)
                .SetFillCache(true)
                .SetVerifyChecksums(false)
                .SetReadaheadSize((ulong)(256 * 1024));

            float localBest = -1f;
            byte[]? localBestKey = null;
            byte[]? localBestVal = null;
            int scanned = 0;

            using var iter = _rocksDb.NewIterator(readOptions: readOpts);
            iter.Seek(prefix);

            while (iter.Valid() && scanned < perBucketBudget)
            {
                var valBytes = iter.Value();
                if (valBytes == null || valBytes.Length < minValueLen) { iter.Next(); continue; }

                int dim = BitConverter.ToInt32(valBytes, 0);
                if (dim != vecLen || valBytes.Length < 4 + dim * sizeof(float)) { iter.Next(); continue; }

                var candidateSpan = MemoryMarshal.Cast<byte, float>(valBytes.AsSpan(4, dim * sizeof(float)));
                float normSq = ExtractNormSquared(valBytes, dim);
                float sim = CosineSimilarityInline(queryVector, queryNormSq, candidateSpan, normSq);
                scanned++;

                if (sim >= threshold && sim > localBest)
                {
                    localBest = sim;
                    localBestKey = iter.Key();
                    localBestVal = valBytes;
                }
                iter.Next();
            }

            perBucket[b] = (localBest, bucketId, localBestKey, localBestVal);
        });

        float bestSim = -1f;
        int bestIdx = -1;
        for (int b = 0; b < bucketCount; b++)
        {
            if (perBucket[b].sim > bestSim)
            {
                bestSim = perBucket[b].sim;
                bestIdx = b;
            }
        }

        if (bestIdx < 0 || perBucket[bestIdx].keyBytes == null || perBucket[bestIdx].valBytes == null)
            return (0, 0, null, -1f);

        ulong bestIndex = BinaryPrimitives.ReadUInt64BigEndian(perBucket[bestIdx].keyBytes!.AsSpan(9));
        string? bestGuid = ExtractStorageGuid(perBucket[bestIdx].valBytes!, vecLen);
        return (perBucket[bestIdx].bucketId, bestIndex, bestGuid, bestSim);
    }

    /// <summary>
    /// Search multiple buckets directly from RocksDB and return the top-K matches
    /// above threshold, sorted descending by similarity. Falls back to the single-best
    /// path when topK == 1.
    /// </summary>
    public List<(ulong bucketId, ulong bucketIndex, string? storageGuid, float similarity)> SearchBucketsDirectTopK(
        IReadOnlyList<ulong> bucketIds, float[] queryVector, float queryNormSq, float threshold, int topK, int maxCandidates = 4096)
    {
        if (topK <= 1)
        {
            var single = SearchBucketsDirect(bucketIds, queryVector, queryNormSq, threshold, maxCandidates);
            if (single.similarity >= threshold && single.storageGuid != null)
                return new List<(ulong, ulong, string?, float)> { single };
            return new List<(ulong, ulong, string?, float)>();
        }

        if (bucketIds.Count == 0)
            return new List<(ulong, ulong, string?, float)>();

        int vecLen = queryVector.Length;
        int vectorBytesLen = vecLen * sizeof(float);
        int minValueLen = 4 + vectorBytesLen;
        int perBucketBudget = Math.Max(64, maxCandidates / Math.Max(1, bucketIds.Count));

        int bucketCount = bucketIds.Count;
        var perBucketHits = new List<(float sim, ulong bucketId, byte[] keyBytes, byte[] valBytes)>[bucketCount];
        for (int i = 0; i < bucketCount; i++)
            perBucketHits[i] = new List<(float, ulong, byte[], byte[])>();

        int maxPar = Math.Min(4, bucketCount);
        Parallel.For(0, bucketCount, new ParallelOptions { MaxDegreeOfParallelism = maxPar }, b =>
        {
            ulong bucketId = bucketIds[b];
            var prefix = MakeBinaryVectorPrefix(bucketId);

            var readOpts = new ReadOptions()
                .SetPrefixSameAsStart(true)
                .SetFillCache(true)
                .SetVerifyChecksums(false)
                .SetReadaheadSize((ulong)(256 * 1024));

            var localHits = new List<(float sim, ulong bucketId, byte[] keyBytes, byte[] valBytes)>();
            float localMinSim = -1f;
            int scanned = 0;

            using var iter = _rocksDb.NewIterator(readOptions: readOpts);
            iter.Seek(prefix);

            while (iter.Valid() && scanned < perBucketBudget)
            {
                var valBytes = iter.Value();
                if (valBytes == null || valBytes.Length < minValueLen) { iter.Next(); continue; }

                int dim = BitConverter.ToInt32(valBytes, 0);
                if (dim != vecLen || valBytes.Length < 4 + dim * sizeof(float)) { iter.Next(); continue; }

                var candidateSpan = MemoryMarshal.Cast<byte, float>(valBytes.AsSpan(4, dim * sizeof(float)));
                float normSq = ExtractNormSquared(valBytes, dim);
                float sim = CosineSimilarityInline(queryVector, queryNormSq, candidateSpan, normSq);
                scanned++;

                if (sim >= threshold)
                {
                    if (localHits.Count < topK || sim > localMinSim)
                    {
                        localHits.Add((sim, bucketId, (byte[])iter.Key().Clone(), (byte[])valBytes.Clone()));
                        if (localHits.Count > topK * 2)
                        {
                            localHits.Sort((a, b) => b.sim.CompareTo(a.sim));
                            localHits.RemoveRange(topK, localHits.Count - topK);
                            localMinSim = localHits[^1].sim;
                        }
                    }
                }
                iter.Next();
            }

            perBucketHits[b] = localHits;
        });

        var merged = new List<(float sim, ulong bucketId, byte[] keyBytes, byte[] valBytes)>();
        for (int b = 0; b < bucketCount; b++)
            merged.AddRange(perBucketHits[b]);

        merged.Sort((a, b) => b.sim.CompareTo(a.sim));

        var seen = new HashSet<string>();
        var results = new List<(ulong bucketId, ulong bucketIndex, string? storageGuid, float similarity)>();
        foreach (var hit in merged)
        {
            if (results.Count >= topK) break;
            ulong idx = BinaryPrimitives.ReadUInt64BigEndian(hit.keyBytes.AsSpan(9));
            string? guid = ExtractStorageGuid(hit.valBytes, vecLen);
            if (guid != null && !seen.Add(guid)) continue;
            results.Add((hit.bucketId, idx, guid, hit.sim));
        }
        return results;
    }

    /// <summary>
    /// Extract normSquared from the tail of a serialized vector record.
    /// Format: [4B dim][dim*4B vector][7-bit-enc strLen][strLen bytes guid][4B chunkSize][4B normSq]
    /// </summary>
    private static float ExtractNormSquared(byte[] valBytes, int dim)
    {
        int strLenOffset = 4 + dim * sizeof(float);
        if (strLenOffset >= valBytes.Length) return 0f;

        // Read BinaryWriter-style 7-bit encoded string length
        int strLen = 0, shift = 0, strLenSize = 0;
        for (int i = strLenOffset; i < valBytes.Length && i < strLenOffset + 5; i++)
        {
            byte b = valBytes[i];
            strLen |= (b & 0x7F) << shift;
            strLenSize++;
            if (b < 128) break;
            shift += 7;
        }

        int normSqOffset = strLenOffset + strLenSize + strLen + 4; // skip guid + chunkSize
        if (normSqOffset + 4 <= valBytes.Length)
            return BitConverter.ToSingle(valBytes, normSqOffset);

        // Fallback: compute from vector bytes
        var vecSpan = MemoryMarshal.Cast<byte, float>(valBytes.AsSpan(4, dim * sizeof(float)));
        return ComputeNormSquaredSpan(vecSpan);
    }

    /// <summary>
    /// Extract storageGuid string from a serialized vector record.
    /// </summary>
    private static string? ExtractStorageGuid(byte[] valBytes, int dim)
    {
        int strLenOffset = 4 + dim * sizeof(float);
        if (strLenOffset >= valBytes.Length) return null;

        int strLen = 0, shift = 0, strLenSize = 0;
        for (int i = strLenOffset; i < valBytes.Length && i < strLenOffset + 5; i++)
        {
            byte b = valBytes[i];
            strLen |= (b & 0x7F) << shift;
            strLenSize++;
            if (b < 128) break;
            shift += 7;
        }

        int strStart = strLenOffset + strLenSize;
        if (strStart + strLen > valBytes.Length || strLen <= 0) return null;
        return Encoding.UTF8.GetString(valBytes, strStart, strLen);
    }

    /// <summary>
    /// SIMD cosine similarity between a float[] query and a ReadOnlySpan&lt;float&gt; candidate.
    /// Uses hardware-accelerated Vector&lt;float&gt; for the dot product.
    /// </summary>
    private static float CosineSimilarityInline(
        float[] queryVec, float queryNormSq,
        ReadOnlySpan<float> candidateSpan, float candidateNormSq)
    {
        int len = queryVec.Length;
        int vecSize = Vector<float>.Count;
        double dot = 0.0;
        int i = 0;

        if (vecSize > 1 && len >= vecSize)
        {
            Vector<float> dotSum = Vector<float>.Zero;
            for (; i <= len - vecSize; i += vecSize)
                dotSum += new Vector<float>(queryVec, i) * new Vector<float>(candidateSpan.Slice(i));
            for (int j = 0; j < vecSize; j++) dot += dotSum[j];
        }

        for (; i < len; i++)
            dot += queryVec[i] * candidateSpan[i];

        const double eps = 1e-12;
        if (queryNormSq <= eps || candidateNormSq <= eps) return 0f;
        double denom = Math.Sqrt(queryNormSq) * Math.Sqrt(candidateNormSq);
        return denom <= eps ? 0f : (float)(dot / denom);
    }

    /// <summary>
    /// Compute squared L2 norm from a ReadOnlySpan&lt;float&gt; using SIMD.
    /// Fallback for records that don't have pre-computed normSquared.
    /// </summary>
    private static float ComputeNormSquaredSpan(ReadOnlySpan<float> vec)
    {
        int vecSize = Vector<float>.Count;
        double norm = 0.0;
        int i = 0;

        if (vecSize > 1 && vec.Length >= vecSize)
        {
            Vector<float> normSum = Vector<float>.Zero;
            for (; i <= vec.Length - vecSize; i += vecSize)
            {
                var v = new Vector<float>(vec.Slice(i));
                normSum += v * v;
            }
            for (int j = 0; j < vecSize; j++) norm += normSum[j];
        }

        for (; i < vec.Length; i++)
            norm += vec[i] * vec[i];
        return (float)norm;
    }

    // ══════════════════════════════════════════════════════════════
    //  DIRECT SEARCH (L1-bypass mode)
    // ══════════════════════════════════════════════════════════════

    /// <summary>
    /// Search a single bucket via RocksDB prefix iterator and return ranked results.
    /// Used by NodeHandler.SearchInBucket when L1 cache is disabled.
    /// </summary>
    public List<M_SearchResult> SearchSingleBucketDirect(
        ulong bucketId, float[] queryVector, float queryNormSq,
        float threshold, int k, int queryIndex)
    {
        int vecLen = queryVector.Length;
        int vectorBytesLen = vecLen * sizeof(float);
        int minValueLen = 4 + vectorBytesLen;

        var readOpts = new ReadOptions()
            .SetPrefixSameAsStart(true)
            .SetFillCache(true)
            .SetVerifyChecksums(false)
            .SetReadaheadSize((ulong)(256 * 1024));

        var prefix = MakeBinaryVectorPrefix(bucketId);
        var topK = new List<(ulong bucketId, ulong bucketIndex, string? guid, float sim)>();

        using var iter = _rocksDb.NewIterator(readOptions: readOpts);
        iter.Seek(prefix);

        while (iter.Valid())
        {
            var valBytes = iter.Value();
            if (valBytes == null || valBytes.Length < minValueLen) { iter.Next(); continue; }

            int dim = BitConverter.ToInt32(valBytes, 0);
            if (dim != vecLen || valBytes.Length < 4 + dim * sizeof(float)) { iter.Next(); continue; }

            var candidateSpan = MemoryMarshal.Cast<byte, float>(valBytes.AsSpan(4, dim * sizeof(float)));
            float normSq = ExtractNormSquared(valBytes, dim);
            float sim = CosineSimilarityInline(queryVector, queryNormSq, candidateSpan, normSq);

            if (sim >= threshold)
            {
                var keyBytes = iter.Key();
                ulong idx = BinaryPrimitives.ReadUInt64BigEndian(keyBytes.AsSpan(9));
                string? guid = ExtractStorageGuid(valBytes, dim);
                topK.Add((bucketId, idx, guid, sim));
            }
            iter.Next();
        }

        topK.Sort((a, b) => b.sim.CompareTo(a.sim));
        int resultCount = Math.Min(k, topK.Count);

        var results = new List<M_SearchResult>(resultCount);
        for (int i = 0; i < resultCount; i++)
        {
            results.Add(new M_SearchResult
            {
                id = topK[i].bucketId,
                index = topK[i].bucketIndex,
                similarity = topK[i].sim,
                chunk = null,
                i = queryIndex
            });
        }
        return results;
    }

    /// <summary>
    /// Within-bucket similarity dedup for the store path.
    /// Scans up to maxRecent vectors in the bucket and returns the best match above threshold.
    /// Used by NodeHandler.StoreInBucket when L1 cache is disabled.
    /// </summary>
    public (ulong bucketId, ulong bucketIndex, string? storageGuid, float similarity)?
        SearchSingleBucketForStore(
            ulong bucketId, float[] queryVector, float queryNormSq,
            float threshold, int maxRecent = 1024)
    {
        int vecLen = queryVector.Length;
        int vectorBytesLen = vecLen * sizeof(float);
        int minValueLen = 4 + vectorBytesLen;

        float bestSim = -1f;
        byte[]? bestKeyBytes = null;
        byte[]? bestValBytes = null;
        int scanned = 0;

        var readOpts = new ReadOptions()
            .SetPrefixSameAsStart(true)
            .SetFillCache(true)
            .SetVerifyChecksums(false)
            .SetReadaheadSize((ulong)(256 * 1024));

        var prefix = MakeBinaryVectorPrefix(bucketId);

        using var iter = _rocksDb.NewIterator(readOptions: readOpts);
        iter.Seek(prefix);

        while (iter.Valid() && scanned < maxRecent)
        {
            var valBytes = iter.Value();
            if (valBytes == null || valBytes.Length < minValueLen) { iter.Next(); continue; }

            int dim = BitConverter.ToInt32(valBytes, 0);
            if (dim != vecLen || valBytes.Length < 4 + dim * sizeof(float)) { iter.Next(); continue; }

            var candidateSpan = MemoryMarshal.Cast<byte, float>(valBytes.AsSpan(4, dim * sizeof(float)));
            float normSq = ExtractNormSquared(valBytes, dim);
            float sim = CosineSimilarityInline(queryVector, queryNormSq, candidateSpan, normSq);
            scanned++;

            if (sim >= threshold && sim > bestSim)
            {
                bestSim = sim;
                bestKeyBytes = iter.Key();
                bestValBytes = valBytes;
            }
            iter.Next();
        }

        if (bestKeyBytes == null || bestValBytes == null)
            return null;

        ulong bestIndex = BinaryPrimitives.ReadUInt64BigEndian(bestKeyBytes.AsSpan(9));
        string? bestGuid = ExtractStorageGuid(bestValBytes, vecLen);
        return (bucketId, bestIndex, bestGuid, bestSim);
    }

    // ══════════════════════════════════════════════════════════════
    //  LANE BUCKET STORAGE (Level 2 sub-chunk index)
    // ══════════════════════════════════════════════════════════════

    /// <summary>
    /// Store 64 lane entries for a freshly stored chunk.
    /// Each lane's mini-LSH bitstring is used as the bucket key.
    /// Uses the existing write batcher — atomically committed with the main vector record.
    /// </summary>
    public void StoreLaneEntries(ushort[] laneHashes, ulong mainBucketId, ulong mainBucketIndex, string storageGuid)
    {
        // Lane counters are bounded (≤65k entries) but must be primed from disk on first
        // use so a restart doesn't reset them to 0 and overwrite existing lane records.
        EnsureLaneCountersLoaded();

        byte[] guidRaw;
        if (!string.IsNullOrEmpty(storageGuid) && storageGuid.Length == 64)
        {
            guidRaw = Convert.FromHexString(storageGuid);
        }
        else
        {
            guidRaw = new byte[32];
        }

        for (int lane = 0; lane < laneHashes.Length; lane++)
        {
            ushort hash = laneHashes[lane];
            ulong idx = _laneNextIndex.GetOrAdd(hash, 0UL);
            _laneNextIndex[hash] = idx + 1;
            Interlocked.Increment(ref _totalLaneEntries);

            // Value: [8 bucketId][8 bucketIndex][1 lanePos][32 storageGuid_raw] = 49 bytes
            var value = new byte[49];
            BitConverter.TryWriteBytes(value.AsSpan(0, 8), mainBucketId);
            BitConverter.TryWriteBytes(value.AsSpan(8, 8), mainBucketIndex);
            value[16] = (byte)lane;
            Buffer.BlockCopy(guidRaw, 0, value, 17, 32);

            var keyBytes = Encoding.UTF8.GetBytes($"{LaneVectorPrefix}{hash}:{idx}");
            _writeBatcher.Put(keyBytes, value);

            // Persist the lane counter
            var nextKeyBytes = Encoding.UTF8.GetBytes($"{LaneNextPrefix}{hash}");
            _writeBatcher.Put(nextKeyBytes, BitConverter.GetBytes(idx + 1));
        }
    }

    /// <summary>
    /// Search a single lane bucket by mini-LSH hash. Returns up to maxResults entries
    /// including the embedded sub-chunk bytes (no chunk loading needed).
    /// Uses a RocksDB prefix iterator — O(1) seek + sequential scan.
    /// </summary>
    public List<(ulong BucketId, ulong BucketIndex, byte LanePos, string StorageGuid, byte[] LaneBytes)> SearchLaneBucket(ushort laneHash, int maxResults = 50)
    {
        var results = new List<(ulong, ulong, byte, string, byte[])>();
        var prefix = Encoding.UTF8.GetBytes($"{LaneVectorPrefix}{laneHash}:");

        using var iterator = _rocksDb.NewIterator();
        iterator.Seek(prefix);

        while (iterator.Valid() && results.Count < maxResults)
        {
            var keyBytes = iterator.Key();
            if (keyBytes.Length < prefix.Length || !keyBytes.AsSpan(0, prefix.Length).SequenceEqual(prefix))
                break;

            var val = iterator.Value();
            if (val != null && val.Length >= 49)
            {
                ulong bucketId = BitConverter.ToUInt64(val, 0);
                ulong bucketIndex = BitConverter.ToUInt64(val, 8);
                byte lanePos = val[16];
                string guid = Convert.ToHexString(val, 17, 32).ToLowerInvariant();

                byte[] laneBytes;
                if (val.Length > 49)
                {
                    laneBytes = new byte[val.Length - 49];
                    Buffer.BlockCopy(val, 49, laneBytes, 0, laneBytes.Length);
                }
                else
                {
                    laneBytes = Array.Empty<byte>();
                }

                results.Add((bucketId, bucketIndex, lanePos, guid, laneBytes));
            }

            iterator.Next();
        }

        return results;
    }

    /// <summary>
    /// Get lane storage statistics from in-memory counters.
    /// </summary>
    public (long totalLaneBuckets, long totalLaneEntries) GetLaneStats()
    {
        EnsureLaneCountersLoaded();
        return (_laneNextIndex.Count, Interlocked.Read(ref _totalLaneEntries));
    }
}
