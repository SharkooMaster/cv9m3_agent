using System.Collections.Concurrent;
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
            .SetWholeKeyFiltering(true)                                     // Bloom on full keys for Get()
            .SetFormatVersion(4);                                           // Latest SST format

        var cfOpts = new ColumnFamilyOptions()
            .SetBlockBasedTableFactory(tableOpts)
            .SetMemtablePrefixBloomSizeRatio(0.1)                           // Memtable bloom for recent writes
            .SetWriteBufferSize(64 * 1024 * 1024)                           // 64MB memtable (vs 4MB default)
            .SetMaxWriteBufferNumber(3)                                     // 3 memtables before stall
            .SetLevel0FileNumCompactionTrigger(4)                           // Compact after 4 L0 files
            .SetCompression(Compression.Zstd);

        var dbOpts = new DbOptions()
            .SetCreateIfMissing(true);

        var columnFamilies = new ColumnFamilies { { "default", cfOpts } };
        _rocksDb = RocksDb.Open(dbOpts, _bucketDbPath, columnFamilies);

        // Initialize write batcher (batches writes in background)
        // High-throughput: Larger batches for better performance
        _writeBatcher = new RocksDbWriteBatcher(_rocksDb, batchSize: 200, flushIntervalMs: 50);

        // Load existing bucket IDs on startup
        LoadBucketIds();

        Console.WriteLine($"[RocksDB Buckets] Initialized at {_bucketDbPath} with bloom filter + {blockCacheSizeMb}MB block cache + LZ4");
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
        using var iterator = _rocksDb.NewIterator();
        iterator.SeekToFirst();

        lock (_bucketIdLock)
        {
            while (iterator.Valid())
            {
                var keyBytes = iterator.Key();
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
                    // Load next-index counters into memory so we never read stale values from RocksDB
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
                    // Load dedup map into memory so we never miss a recently-stored entry
                    var valueBytes = iterator.Value();
                    if (valueBytes != null && valueBytes.Length == sizeof(ulong))
                    {
                        var dedupSuffix = key.Substring(BucketStorageGuidPrefix.Length); // "{bucketId}:{storageGuid}"
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
            var vectorKeyBytes = Encoding.UTF8.GetBytes($"{BucketVectorPrefix}{bucketId}:{bucketIndex}");
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

        foreach (var bucketName in bucketNames)
        {
            if (!_bucketNameToId.TryGetValue(bucketName, out var bucketId) || bucketId == 0)
                continue;

            // Prefix iterator: 1 seek + sequential scan (fast) instead of N random Get() calls (slow)
            var prefix = Encoding.UTF8.GetBytes($"{BucketVectorPrefix}{bucketId}:");
            using var iterator = _rocksDb.NewIterator();
            iterator.Seek(prefix);

            while (iterator.Valid())
            {
                var keyBytes = iterator.Key();
                if (keyBytes.Length < prefix.Length || !keyBytes.AsSpan(0, prefix.Length).SequenceEqual(prefix))
                    break;

                var indexStr = Encoding.UTF8.GetString(keyBytes, prefix.Length, keyBytes.Length - prefix.Length);
                if (!ulong.TryParse(indexStr, out var bucketIndex))
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
        var key = Encoding.UTF8.GetBytes($"{BucketVectorPrefix}{bucketId}:{bucketIndex}");
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

        var prefix = Encoding.UTF8.GetBytes($"{BucketVectorPrefix}{bucketId}:");
        var vectors = new List<(float[], string, ulong, ulong, float)>();

        using var iterator = _rocksDb.NewIterator();
        iterator.Seek(prefix);

        while (iterator.Valid())
        {
            var keyBytes = iterator.Key();
            if (keyBytes.Length < prefix.Length || !keyBytes.AsSpan(0, prefix.Length).SequenceEqual(prefix))
                break;

            var indexStr = Encoding.UTF8.GetString(keyBytes, prefix.Length, keyBytes.Length - prefix.Length);
            if (!ulong.TryParse(indexStr, out var bucketIndex))
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

        // Single iterator scan over all "bv:" keys — sequential I/O, very fast
        var globalPrefix = Encoding.UTF8.GetBytes(BucketVectorPrefix);
        using var iterator = _rocksDb.NewIterator();
        iterator.Seek(globalPrefix);

        // Reverse map: bucketId → bucketName (needed for result keys)
        Dictionary<ulong, string> idToName;
        lock (_bucketIdLock)
        {
            idToName = new Dictionary<ulong, string>(_bucketIdToName);
        }

        while (iterator.Valid())
        {
            var keyBytes = iterator.Key();
            if (keyBytes.Length < globalPrefix.Length || !keyBytes.AsSpan(0, globalPrefix.Length).SequenceEqual(globalPrefix))
                break;

            // Key format: "bv:{bucketId}:{index}"
            var keySuffix = Encoding.UTF8.GetString(keyBytes, globalPrefix.Length, keyBytes.Length - globalPrefix.Length);
            var colonIdx = keySuffix.IndexOf(':');
            if (colonIdx < 0)
            {
                iterator.Next();
                continue;
            }

            if (!ulong.TryParse(keySuffix.AsSpan(0, colonIdx), out var bucketId) ||
                !ulong.TryParse(keySuffix.AsSpan(colonIdx + 1), out var bucketIndex))
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
