using System.Collections.Concurrent;
using System.Text;
using System.Text.Json;
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
    private static readonly object _bucketIdLock = new object(); // Only for bucket ID allocation
    private static ulong _nextBucketId = 1;
    private static readonly Dictionary<string, ulong> _bucketNameToId = new();
    // Per-bucket locks to allow concurrent writes to different buckets
    private static readonly ConcurrentDictionary<string, object> _bucketLocks = new();
    // Index: bucketId -> bucketName for fast reverse lookups (O(1) instead of O(n) scan)
    private static readonly ConcurrentDictionary<ulong, string> _bucketIdToName = new();

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
        // Scan all bucket keys to rebuild bucket ID mapping
        using var iterator = _rocksDb.NewIterator();
        iterator.SeekToFirst();
        
        lock (_bucketIdLock)
        {
            while (iterator.Valid())
            {
                var keyBytes = iterator.Key();
                var key = Encoding.UTF8.GetString(keyBytes);
                if (key.StartsWith("bucket:"))
                {
                    var bucketName = key.Substring(7); // "bucket:".Length
                    var valueBytes = iterator.Value();
                    try
                    {
                        var bucketData = JsonSerializer.Deserialize<BucketData>(Encoding.UTF8.GetString(valueBytes));
                        if (bucketData != null && bucketData.BucketId > 0)
                        {
                            _bucketNameToId[bucketName] = bucketData.BucketId;
                            _bucketIdToName[bucketData.BucketId] = bucketName; // Build reverse index
                            if (bucketData.BucketId >= _nextBucketId)
                                _nextBucketId = bucketData.BucketId + 1;
                        }
                    }
                    catch { /* Skip invalid entries */ }
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
            return id;
        }
    }

    /// <summary>
    /// Store a vector in a bucket. Returns (bucketId, bucketIndex).
    /// Thread-safe: uses per-bucket locking to allow concurrent writes to different buckets.
    /// </summary>
    public (ulong bucketId, ulong bucketIndex) StoreVector(string bucketName, float[] vector, string storageGuid, int chunkSize)
    {
        var key = Encoding.UTF8.GetBytes($"bucket:{bucketName}");
        
        // Get per-bucket lock (allows concurrent writes to different buckets)
        var bucketLock = _bucketLocks.GetOrAdd(bucketName, _ => new object());
        
        // Thread-safe read-modify-write for THIS bucket only
        lock (bucketLock)
        {
            // Get or create bucket ID (quick operation, separate lock)
            ulong bucketId;
            lock (_bucketIdLock)
            {
                bucketId = GetOrCreateBucketId(bucketName);
            }
            
            // Read existing bucket data (outside bucketIdLock, but inside bucketLock)
            var existingJson = _rocksDb.Get(key);
            BucketData bucketData;
            
            if (existingJson != null && existingJson.Length > 0)
            {
                try
                {
                    bucketData = JsonSerializer.Deserialize<BucketData>(Encoding.UTF8.GetString(existingJson)) ?? new BucketData();
                    // Ensure bucket ID is set (in case it wasn't in the loaded data)
                    if (bucketData.BucketId == 0)
                        bucketData.BucketId = bucketId;
                }
                catch
                {
                    bucketData = new BucketData { BucketId = bucketId };
                }
            }
            else
            {
                bucketData = new BucketData { BucketId = bucketId };
            }

            // Check for duplicate storageGuid
            var existingIndex = bucketData.Vectors.FindIndex(v => v.StorageGuid == storageGuid);
            if (existingIndex >= 0)
            {
                return (bucketData.BucketId, (ulong)existingIndex);
            }

            // Add new vector
            var bucketIndex = (ulong)bucketData.Vectors.Count;
            bucketData.Vectors.Add(new VectorData
            {
                Vector = vector,
                StorageGuid = storageGuid,
                ChunkSize = chunkSize
            });

            // Write back to RocksDB (batched, non-blocking)
            var json = JsonSerializer.Serialize(bucketData);
            _writeBatcher.Put(key, Encoding.UTF8.GetBytes(json));

            return (bucketData.BucketId, bucketIndex);
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
            var key = Encoding.UTF8.GetBytes($"bucket:{bucketName}");
            var json = _rocksDb.Get(key);
            
            if (json == null || json.Length == 0)
                continue;

            try
            {
                var bucketData = JsonSerializer.Deserialize<BucketData>(Encoding.UTF8.GetString(json));
                if (bucketData == null)
                    continue;

                for (ulong i = 0; i < (ulong)bucketData.Vectors.Count; i++)
                {
                    var vec = bucketData.Vectors[(int)i];
                    results.Add((vec.Vector, vec.StorageGuid, bucketData.BucketId, i, bucketName));
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[RocksDB Buckets] Error reading bucket {bucketName}: {ex.Message}");
            }
        }

        return results;
    }

    /// <summary>
    /// Get storage GUID by bucket ID and index.
    /// OPTIMIZED: Uses reverse index (bucketId -> bucketName) for O(1) lookup instead of O(n) scan.
    /// </summary>
    public string? GetStorageGuidByReference(ulong bucketId, ulong bucketIndex)
    {
        // Fast path: Use reverse index to get bucket name directly (O(1))
        string? bucketName = null;
        lock (_bucketIdLock)
        {
            _bucketIdToName.TryGetValue(bucketId, out bucketName);
        }
        
        if (bucketName == null)
        {
            // Fallback: bucket not in index (shouldn't happen, but handle gracefully)
            // Could rebuild index or do linear search, but for now return null
            return null;
        }
        
        // Direct lookup by bucket name (O(1) RocksDB lookup)
        var key = Encoding.UTF8.GetBytes($"bucket:{bucketName}");
        var jsonBytes = _rocksDb.Get(key);
        
        if (jsonBytes == null || jsonBytes.Length == 0)
            return null;
        
        try
        {
            var bucketData = JsonSerializer.Deserialize<BucketData>(Encoding.UTF8.GetString(jsonBytes));
            if (bucketData != null && (ulong)bucketData.Vectors.Count > bucketIndex)
            {
                return bucketData.Vectors[(int)bucketIndex].StorageGuid;
            }
        }
        catch { /* Skip invalid entries */ }
        
        return null;
    }

    private class BucketData
    {
        public ulong BucketId { get; set; }
        public List<VectorData> Vectors { get; set; } = new();
    }

    private class VectorData
    {
        public float[] Vector { get; set; } = Array.Empty<float>();
        public string StorageGuid { get; set; } = string.Empty;
        public int ChunkSize { get; set; }
    }
}
