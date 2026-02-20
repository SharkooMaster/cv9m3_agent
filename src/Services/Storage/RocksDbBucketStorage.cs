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
    private static ulong _nextBucketId = 1;
    private static readonly Dictionary<string, ulong> _bucketNameToId = new();
    private static readonly ConcurrentDictionary<string, object> _bucketLocks = new();
    private static readonly ConcurrentDictionary<ulong, string> _bucketIdToName = new();

    // New append-friendly schema:
    // bn:{bucketName}              -> ulong bucketId
    // bi:{bucketId}                -> bucketName (utf8)
    // bnext:{bucketId}             -> ulong next vector index
    // bsg:{bucketId}:{storageGuid} -> ulong existing vector index (dedup)
    // bv:{bucketId}:{index}        -> binary record: [int dim][float * dim][string storageGuid][int chunkSize]
    private const string BucketNameToIdPrefix = "bn:";
    private const string BucketIdToNamePrefix = "bi:";
    private const string BucketNextPrefix = "bnext:";
    private const string BucketStorageGuidPrefix = "bsg:";
    private const string BucketVectorPrefix = "bv:";

    public RocksDbBucketStorage(string basePath)
    {
        _bucketDbPath = Path.Combine(basePath, "buckets");
        Directory.CreateDirectory(_bucketDbPath);
        var options = new DbOptions().SetCreateIfMissing(true);
        _rocksDb = RocksDb.Open(options, _bucketDbPath);
        
        // Initialize write batcher (batches writes in background)
        // High-throughput: Larger batches for better performance
        _writeBatcher = new RocksDbWriteBatcher(_rocksDb, batchSize: 200, flushIntervalMs: 50);
        
        // Load existing bucket IDs on startup
        LoadBucketIds();
        
        Console.WriteLine($"[RocksDB Buckets] Initialized at {_bucketDbPath} with write batching");
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
                    var idBytes = iterator.Value();
                    if (idBytes != null && idBytes.Length == sizeof(ulong))
                    {
                        var bucketId = BitConverter.ToUInt64(idBytes, 0);
                        if (bucketId > 0)
                        {
                            _bucketNameToId[bucketName] = bucketId;
                            _bucketIdToName[bucketId] = bucketName;
                            if (bucketId >= _nextBucketId)
                                _nextBucketId = bucketId + 1;
                        }
                    }
                }
                iterator.Next();
            }
        }
    }

    /// <summary>
    /// Get or create bucket ID for a bucket name.
    /// </summary>
    private ulong GetOrCreateBucketId(string bucketName)
    {
        lock (_bucketIdLock)
        {
            if (_bucketNameToId.TryGetValue(bucketName, out var id))
                return id;

            id = _nextBucketId++;
            _bucketNameToId[bucketName] = id;
            _bucketIdToName[id] = bucketName;

            _writeBatcher.Put(Encoding.UTF8.GetBytes($"{BucketNameToIdPrefix}{bucketName}"), BitConverter.GetBytes(id));
            _writeBatcher.Put(Encoding.UTF8.GetBytes($"{BucketIdToNamePrefix}{id}"), Encoding.UTF8.GetBytes(bucketName));
            _writeBatcher.Put(Encoding.UTF8.GetBytes($"{BucketNextPrefix}{id}"), BitConverter.GetBytes((ulong)0));

            return id;
        }
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

            var dedupKey = $"{BucketStorageGuidPrefix}{bucketId}:{storageGuid}";
            var existingIndexBytes = _rocksDb.Get(Encoding.UTF8.GetBytes(dedupKey));
            if (existingIndexBytes != null && existingIndexBytes.Length == sizeof(ulong))
            {
                return (bucketId, BitConverter.ToUInt64(existingIndexBytes, 0));
            }

            var nextKeyBytes = Encoding.UTF8.GetBytes($"{BucketNextPrefix}{bucketId}");
            var nextBytes = _rocksDb.Get(nextKeyBytes);
            ulong bucketIndex = (nextBytes != null && nextBytes.Length == sizeof(ulong))
                ? BitConverter.ToUInt64(nextBytes, 0)
                : 0UL;

            var recordBytes = SerializeVectorRecord(vector, storageGuid, chunkSize);
            var vectorKeyBytes = Encoding.UTF8.GetBytes($"{BucketVectorPrefix}{bucketId}:{bucketIndex}");

            _writeBatcher.Put(vectorKeyBytes, recordBytes);
            _writeBatcher.Put(Encoding.UTF8.GetBytes(dedupKey), BitConverter.GetBytes(bucketIndex));
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

            var nextKey = Encoding.UTF8.GetBytes($"{BucketNextPrefix}{bucketId}");
            var nextBytes = _rocksDb.Get(nextKey);
            if (nextBytes == null || nextBytes.Length != sizeof(ulong))
                continue;

            var nextIndex = BitConverter.ToUInt64(nextBytes, 0);
            for (ulong i = 0; i < nextIndex; i++)
            {
                var vectorKey = Encoding.UTF8.GetBytes($"{BucketVectorPrefix}{bucketId}:{i}");
                var recordBytes = _rocksDb.Get(vectorKey);
                if (recordBytes == null || recordBytes.Length == 0)
                    continue;

                if (TryDeserializeVectorRecord(recordBytes, out var rec))
                {
                    results.Add((rec.Vector, rec.StorageGuid, bucketId, i, bucketName));
                }
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

    private static byte[] SerializeVectorRecord(float[] vector, string storageGuid, int chunkSize)
    {
        using var ms = new MemoryStream();
        using var writer = new BinaryWriter(ms, Encoding.UTF8, leaveOpen: true);
        writer.Write(vector.Length);
        for (int i = 0; i < vector.Length; i++)
            writer.Write(vector[i]);
        writer.Write(storageGuid ?? string.Empty);
        writer.Write(chunkSize);
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

            rec = new VectorRecord(vector, storageGuid, chunkSize);
            return true;
        }
        catch
        {
            return false;
        }
    }

    private readonly record struct VectorRecord(float[] Vector, string StorageGuid, int ChunkSize);
}
