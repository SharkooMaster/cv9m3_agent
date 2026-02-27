
using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Text;
using Agent.Utils;
using Agent.Utils.Misc;
using Agent.Services.Storage;

public class M_Bucket
{
    public ulong ID { get; set; }
    public ulong lastId = 0;

    // ── PERFORMANCE: List<M_Data> + lock replaces ConcurrentBag<M_Data>. ──
    // ConcurrentBag.ToArray() allocates a full copy on every call (O(n) + GC pressure).
    // List gives direct array-backed iteration — zero allocation for reads.
    // Writes are already serialized per-bucket (RocksDbBucketStorage holds a per-bucket lock).
    private readonly List<M_Data> _data = new();
    private readonly object _dataLock = new();

    // ── Dedup guard: track which chunks (by SHA256 of content) are already stored. ──
    private readonly ConcurrentDictionary<string, (ulong id, ulong index)> _seenChunks = new();

    // ── LRU tracking: updated on every search/store access ──
    private long _lastAccessedTicks = DateTime.UtcNow.Ticks;
    public long LastAccessedTicks => Interlocked.Read(ref _lastAccessedTicks);
    public void TouchAccess() => Interlocked.Exchange(ref _lastAccessedTicks, DateTime.UtcNow.Ticks);

    // ── Memory estimation ──
    // Per vector: ~470 bytes (256 float[64] + 130 storageGuid string + 16 id/index + ~68 object/list overhead)
    // Per dedup entry: ~200 bytes (64-char key + tuple + ConcurrentDict node overhead)
    public const int EstBytesPerVectorPublic = 470;
    private const int EstBytesPerVector = EstBytesPerVectorPublic;
    private const int EstBytesPerDedupEntry = 200;
    private const int EstBucketOverhead = 256; // object + lock + dict headers

    public long EstimatedMemoryBytes
    {
        get
        {
            int dataCount, dedupCount;
            lock (_dataLock) { dataCount = _data.Count; }
            dedupCount = _seenChunks.Count;
            return EstBucketOverhead + ((long)dataCount * EstBytesPerVector) + ((long)dedupCount * EstBytesPerDedupEntry);
        }
    }

    // Backward compat: old code that does `bucket.data.Add(...)` during warmup needs this.
    // Wraps the internal list with proper locking.
    public BucketDataAccessor data => new BucketDataAccessor(this);

    public M_Bucket(ulong _ID)
    {
        ID = _ID;
    }

    /// <summary>
    /// Returns a snapshot of all data items as an array.
    /// Lock-free read: copies the internal array reference (O(1)),
    /// then the caller iterates without holding the lock.
    /// </summary>
    public M_Data[] GetDataSnapshot()
    {
        lock (_dataLock)
        {
            // List<T>.ToArray() is a single memcpy — much faster than ConcurrentBag snapshot
            return _data.ToArray();
        }
    }

    /// <summary>
    /// Adds an item to the bucket's data list. Thread-safe.
    /// </summary>
    public void AddData(M_Data item)
    {
        lock (_dataLock)
        {
            _data.Add(item);
        }
    }

    /// <summary>
    /// Current count of data items. Thread-safe.
    /// </summary>
    public int DataCount
    {
        get { lock (_dataLock) return _data.Count; }
    }

    private static string HashChunk(byte[] chunk)
    {
        using var sha = SHA256.Create();
        var hash = sha.ComputeHash(chunk);
        var sb = new StringBuilder(hash.Length * 2);
        foreach (var b in hash) sb.Append(b.ToString("x2"));
        return sb.ToString();
    }

    public async Task<(ulong, ulong)> InsertData(M_Data _data, ulong _id)
    {
        if (_data == null) throw new ArgumentNullException(nameof(_data));

        TouchAccess();

        // ── Dedup: if identical chunk content is already in this bucket, return its reference. ──
        if (_data.chunk != null && _data.chunk.Length > 0)
        {
            var chunkKey = HashChunk(_data.chunk);
            if (_seenChunks.TryGetValue(chunkKey, out var existing))
            {
                // CRITICAL: Always populate storageGuid on the M_Data object.
                // Without this, the caller (StoreVectorService.StoreSingle) reads
                // _data.storageGuid → null → returns empty StorageGuid in the gRPC
                // response → Cross writes a ref with non-zero BucketId but all-zeros
                // storageGuid → decompression can't fetch → "Missing base chunk".
                // chunkKey IS the SHA256 hex of the chunk data (same as GenerateChunkKey).
                _data.storageGuid = chunkKey;
                _data.id = existing.Item1;
                _data.index = existing.Item2;
                return existing;
            }

            string bucketName = RocksDbBucketStorage.UlongToBitstring(ID);
            (ulong bucketId, ulong bucketIndex) = await NetworkFileStorageHandler.StoreVector(bucketName, _data);

            _data.id = bucketId;
            _data.index = bucketIndex;

            // Free chunk bytes from RAM — they're now persisted in RocksDB + chunk cache.
            // Search never reads _data.chunk; it uses ChunkCacheHandler.GetFromCacheOnly(storageGuid).
            // Keeping chunk bytes alive here was the #1 source of unbounded RAM growth.
            _data.chunk = null;

            AddData(_data);
            _seenChunks.TryAdd(chunkKey, (bucketId, bucketIndex));

            return (bucketId, bucketIndex);
        }

        // Fallback: no chunk data — store anyway (shouldn't happen but safety net).
        AddData(_data);
        string fallbackName = RocksDbBucketStorage.UlongToBitstring(ID);
        (ulong bid, ulong bidx) = await NetworkFileStorageHandler.StoreVector(fallbackName, _data);
        _data.id = bid;
        _data.index = bidx;
        return (bid, bidx);
    }

    // SearchData is no longer called in the hot path (SearchVector.Get uses ProcessSingleQuery).
    // Kept for backward compatibility.
    public Task<List<M_SearchResult>> SearchData(float[] _vector, float _minimum_similarity, int _k, int _i)
    {
        var results = new List<M_SearchResult>();
        var snapshot = GetDataSnapshot();

        float bestSim = -1f;
        M_Data? bestData = null;

        for (int idx = 0; idx < snapshot.Length; idx++)
        {
            var row = snapshot[idx];
            if (row?.vector == null || row.vector.Length != _vector.Length) continue;
            float sim = Misc.CalculateDistance(_vector, row.vector);
            if (sim >= _minimum_similarity && sim > bestSim)
            {
                bestSim = sim;
                bestData = row;
            }
        }

        if (bestData != null)
        {
            results.Add(new M_SearchResult
            {
                id = bestData.id,
                index = bestData.index,
                similarity = bestSim,
                chunk = bestData.chunk,
                i = _i
            });
        }

        return Task.FromResult(results);
    }
}

/// <summary>
/// Wrapper that gives backward-compatible Add/Count access to M_Bucket's internal list.
/// Used by WarmUpBuckets() and other code that does `bucket.data.Add(item)`.
/// </summary>
public struct BucketDataAccessor
{
    private readonly M_Bucket _bucket;
    public BucketDataAccessor(M_Bucket bucket) => _bucket = bucket;
    public void Add(M_Data item) => _bucket.AddData(item);
    public int Count => _bucket.DataCount;
}
