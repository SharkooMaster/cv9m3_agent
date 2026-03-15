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
    private static readonly object _bucketIdLock = new object();
    private static readonly Dictionary<string, ulong> _bucketNameToId = new();
    private static readonly ConcurrentDictionary<string, object> _bucketLocks = new();
    private static readonly ConcurrentDictionary<ulong, string> _bucketIdToName = new();

    // ── IN-MEMORY counters & dedup: fixes write-batcher read-after-write race ──
    // The write batcher flushes to RocksDB every 50ms. Two rapid StoreVector calls
    // for the same bucket would both read the same stale `next` counter from RocksDB
    // → duplicate bucketIndex → wrong base chunk during decompression → CORRUPTION.
    // Fix: track counters and dedup entries in memory (protected by per-bucket lock).
    private static readonly ConcurrentDictionary<ulong, ulong> _nextIndexInMemory = new();
    private static readonly ConcurrentDictionary<string, ulong> _dedupInMemory = new();

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

    // Binary key format for bucket vectors (replaces string "bv:{id}:{idx}")
    // [0x01][8-byte BE bucketId][8-byte BE index] = 17 bytes total, 9-byte prefix
    private const byte BinaryVectorTag = 0x01;
    private const int BinaryKeyLen = 17;
    private const int BinaryPrefixLen = 9;
    private static readonly byte[] MigrationMarkerKey = Encoding.UTF8.GetBytes("__bv_binary_v1__");

    // In-memory lane counters (same pattern as _nextIndexInMemory)
    private static readonly ConcurrentDictionary<ushort, ulong> _laneNextIndex = new();

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
            .SetMaxBackgroundFlushes(Math.Max(2, bgThreads / 2));

        var columnFamilies = new ColumnFamilies { { "default", cfOpts } };
        _rocksDb = RocksDb.Open(dbOpts, _bucketDbPath, columnFamilies);

        // Initialize write batcher (batches writes in background)
        // High-throughput: Larger batches for better performance
        _writeBatcher = new RocksDbWriteBatcher(_rocksDb, batchSize: 10_000, flushIntervalMs: 5_000);

        // Load existing bucket IDs on startup
        LoadBucketIds();

        // One-time migration: convert string bv: keys to binary format for prefix bloom
        MigrateToBinaryKeys();

        Console.WriteLine($"[RocksDB Buckets] Initialized at {_bucketDbPath} with prefix bloom + {blockCacheSizeMb}MB block cache + LZ4");
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
        _writeBatcher?.Flush(); // Flush pending writes before shutdown
        _writeBatcher?.Dispose();
        _rocksDb?.Dispose();
    }

    private void LoadBucketIds()
    {
        // Total-order seek is required because a prefix extractor is set on this CF.
        // Without it, SeekToFirst would only iterate within the first prefix.
        var readOpts = new ReadOptions().SetTotalOrderSeek(true);
        using var iterator = _rocksDb.NewIterator(readOptions: readOpts);

        // Seek past binary vector keys (0x01...) directly to metadata keys (0x02+)
        iterator.Seek(new byte[] { BinaryVectorTag + 1 });

        lock (_bucketIdLock)
        {
            while (iterator.Valid())
            {
                var keyBytes = iterator.Key();
                if (keyBytes == null || keyBytes.Length == 0) { iterator.Next(); continue; }

                var key = Encoding.UTF8.GetString(keyBytes);

                if (key.StartsWith(BucketNameToIdPrefix, StringComparison.Ordinal))
                {
                    var bucketName = key.Substring(BucketNameToIdPrefix.Length);
                    var bucketId = BitstringToUlong(bucketName);
                    if (bucketId > 0)
                    {
                        _bucketNameToId[bucketName] = bucketId;
                        _bucketIdToName[bucketId] = bucketName;
                    }
                }
                else if (key.StartsWith(BucketNextPrefix, StringComparison.Ordinal))
                {
                    var valueBytes = iterator.Value();
                    if (valueBytes != null && valueBytes.Length == sizeof(ulong))
                    {
                        var idStr = key.Substring(BucketNextPrefix.Length);
                        if (ulong.TryParse(idStr, out var bucketId))
                            _nextIndexInMemory[bucketId] = BitConverter.ToUInt64(valueBytes, 0);
                    }
                }
                else if (key.StartsWith(BucketStorageGuidPrefix, StringComparison.Ordinal))
                {
                    var valueBytes = iterator.Value();
                    if (valueBytes != null && valueBytes.Length == sizeof(ulong))
                    {
                        var dedupSuffix = key.Substring(BucketStorageGuidPrefix.Length);
                        _dedupInMemory[dedupSuffix] = BitConverter.ToUInt64(valueBytes, 0);
                    }
                }
                else if (key.StartsWith(LaneNextPrefix, StringComparison.Ordinal))
                {
                    var valueBytes = iterator.Value();
                    if (valueBytes != null && valueBytes.Length == sizeof(ulong))
                    {
                        var hashStr = key.Substring(LaneNextPrefix.Length);
                        if (ushort.TryParse(hashStr, out var laneHash))
                            _laneNextIndex[laneHash] = BitConverter.ToUInt64(valueBytes, 0);
                    }
                }

                iterator.Next();
            }
        }

        Console.WriteLine($"[RocksDB Buckets] Loaded {_bucketNameToId.Count} buckets, {_nextIndexInMemory.Count} counters, {_dedupInMemory.Count} dedup entries, {_laneNextIndex.Count} lane counters into memory");
    }

    /// <summary>
    /// Get or create bucket ID for a bucket name (64-char bitstring).
    /// The ID is deterministic: bitstring → ulong (1:1 bijection).
    /// This makes bucket IDs GLOBALLY unique — the same bitstring on any agent
    /// produces the same ID, enabling decompression to route via RendezvousRouter.
    /// </summary>
    private ulong GetOrCreateBucketId(string bucketName)
    {
        lock (_bucketIdLock)
        {
            if (_bucketNameToId.TryGetValue(bucketName, out var id))
                return id;

            id = BitstringToUlong(bucketName);
            _bucketNameToId[bucketName] = id;
            _bucketIdToName[id] = bucketName;

            // Seed the in-memory next counter for this brand-new bucket
            _nextIndexInMemory.TryAdd(id, 0UL);

            _writeBatcher.Put(Encoding.UTF8.GetBytes($"{BucketNameToIdPrefix}{bucketName}"), BitConverter.GetBytes(id));
            _writeBatcher.Put(Encoding.UTF8.GetBytes($"{BucketIdToNamePrefix}{id}"), Encoding.UTF8.GetBytes(bucketName));
            _writeBatcher.Put(Encoding.UTF8.GetBytes($"{BucketNextPrefix}{id}"), BitConverter.GetBytes((ulong)0));

            return id;
        }
    }

    /// <summary>
    /// Convert a 64-char '0'/'1' bitstring into a ulong. 1:1 bijection — no collisions.
    /// Bit 0 of the string maps to bit 0 of the ulong, etc.
    /// </summary>
    internal static ulong BitstringToUlong(string bitstring)
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
    /// Thread-safe: uses per-bucket locking to allow concurrent writes to different buckets.
    /// </summary>
    public (ulong bucketId, ulong bucketIndex) StoreVector(string bucketName, float[] vector, string storageGuid, int chunkSize)
    {
        var bucketLock = _bucketLocks.GetOrAdd(bucketName, _ => new object());

        lock (bucketLock)
        {
            var bucketId = GetOrCreateBucketId(bucketName);

            // ── Dedup check: use IN-MEMORY map (never stale, unlike RocksDB batcher) ──
            var dedupSuffix = $"{bucketId}:{storageGuid}";
            if (_dedupInMemory.TryGetValue(dedupSuffix, out var existingIndex))
            {
                return (bucketId, existingIndex);
            }

            // ── Next index: use IN-MEMORY counter (never stale) ──
            ulong bucketIndex = _nextIndexInMemory.GetOrAdd(bucketId, 0UL);

            // Increment the in-memory counter IMMEDIATELY (before releasing the lock)
            _nextIndexInMemory[bucketId] = bucketIndex + 1;

            // Track in dedup map IMMEDIATELY
            _dedupInMemory[dedupSuffix] = bucketIndex;

            // Persist to RocksDB via batcher (background, eventual consistency for durability)
            var recordBytes = SerializeVectorRecord(vector, storageGuid, chunkSize);
            var vectorKeyBytes = MakeBinaryVectorKey(bucketId, bucketIndex);
            var nextKeyBytes = Encoding.UTF8.GetBytes($"{BucketNextPrefix}{bucketId}");

            _writeBatcher.Put(vectorKeyBytes, recordBytes);
            _writeBatcher.Put(Encoding.UTF8.GetBytes($"{BucketStorageGuidPrefix}{dedupSuffix}"), BitConverter.GetBytes(bucketIndex));
            _writeBatcher.Put(nextKeyBytes, BitConverter.GetBytes(bucketIndex + 1));

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
            .SetFillCache(true);

        foreach (var bucketName in bucketNames)
        {
            if (!_bucketNameToId.TryGetValue(bucketName, out var bucketId) || bucketId == 0)
                continue;

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
        ulong bucketId;
        lock (_bucketIdLock)
        {
            if (!_bucketNameToId.TryGetValue(bucketName, out bucketId) || bucketId == 0)
                return null;
        }

        var prefix = MakeBinaryVectorPrefix(bucketId);
        var vectors = new List<(float[], string, ulong, ulong, float)>();
        var readOpts = new ReadOptions()
            .SetPrefixSameAsStart(true)
            .SetFillCache(true);

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

        Dictionary<ulong, string> idToName;
        lock (_bucketIdLock)
        {
            idToName = new Dictionary<ulong, string>(_bucketIdToName);
        }

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
                if (idToName.TryGetValue(bucketId, out var bucketName))
                {
                    if (!result.TryGetValue(bucketName, out var list))
                    {
                        list = new List<(float[], string, ulong, ulong, float)>();
                        result[bucketName] = list;
                    }
                    list.Add((rec.Vector, rec.StorageGuid, bucketId, bucketIndex, rec.NormSquared));
                }
            }

            iterator.Next();
        }

        return result;
    }

    /// <summary>
    /// Get fast O(1) bucket and vector counts from in-memory maps.
    /// </summary>
    public (long totalBuckets, long totalVectors) GetBucketAndVectorStats()
    {
        long totalBuckets;
        lock (_bucketIdLock)
        {
            totalBuckets = _bucketNameToId.Count;
        }

        long totalVectors = 0;
        foreach (var kv in _nextIndexInMemory)
            totalVectors += (long)kv.Value;

        return (totalBuckets, totalVectors);
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
            .SetVerifyChecksums(false);

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
        int vecLen = queryVector.Length;
        int vectorBytesLen = vecLen * sizeof(float);
        int minValueLen = 4 + vectorBytesLen;

        float bestSim = -1f;
        ulong bestBucketId = 0;
        byte[]? bestKeyBytes = null;
        byte[]? bestValBytes = null;
        int totalScanned = 0;

        var readOpts = new ReadOptions()
            .SetPrefixSameAsStart(true)
            .SetFillCache(true)
            .SetVerifyChecksums(false);

        for (int b = 0; b < bucketIds.Count && totalScanned < maxCandidates; b++)
        {
            ulong bucketId = bucketIds[b];
            var prefix = MakeBinaryVectorPrefix(bucketId);

            using var iter = _rocksDb.NewIterator(readOptions: readOpts);
            iter.Seek(prefix);

            while (iter.Valid() && totalScanned < maxCandidates)
            {
                var valBytes = iter.Value();
                if (valBytes == null || valBytes.Length < minValueLen)
                {
                    iter.Next();
                    continue;
                }

                int dim = BitConverter.ToInt32(valBytes, 0);
                if (dim != vecLen || valBytes.Length < 4 + dim * sizeof(float))
                {
                    iter.Next();
                    continue;
                }

                var candidateSpan = MemoryMarshal.Cast<byte, float>(
                    valBytes.AsSpan(4, dim * sizeof(float)));
                float normSq = ExtractNormSquared(valBytes, dim);
                float sim = CosineSimilarityInline(queryVector, queryNormSq, candidateSpan, normSq);
                totalScanned++;

                if (sim >= threshold && sim > bestSim)
                {
                    bestSim = sim;
                    bestBucketId = bucketId;
                    bestKeyBytes = iter.Key();
                    bestValBytes = valBytes;
                }

                iter.Next();
            }
        }

        if (bestKeyBytes == null || bestValBytes == null)
            return (0, 0, null, -1f);

        ulong bestIndex = BinaryPrimitives.ReadUInt64BigEndian(bestKeyBytes.AsSpan(9));
        string? bestGuid = ExtractStorageGuid(bestValBytes, vecLen);
        return (bestBucketId, bestIndex, bestGuid, bestSim);
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
            .SetVerifyChecksums(false);

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
            .SetVerifyChecksums(false);

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
        long totalEntries = 0;
        foreach (var kv in _laneNextIndex)
            totalEntries += (long)kv.Value;
        return (_laneNextIndex.Count, totalEntries);
    }
}
