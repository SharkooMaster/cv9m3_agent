
using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Text;
using Agent.Utils;
using Agent.Utils.Globals;
using Agent.Utils.Misc;
using Agent.Services.Storage;
using Agent.Modules.Storage;

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

    // ── Per-bucket store semaphore: serializes check-then-store within a bucket. ──
    // Prevents the TOCTOU race where two threads both pass the dedup check and
    // both store. Only same-bucket stores contend; different buckets are fully parallel.
    private readonly SemaphoreSlim _storeSemaphore = new(1, 1);

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
        if (item.normSquared == 0f && item.vector != null && item.vector.Length > 0)
            item.normSquared = Misc.ComputeNormSquared(item.vector);
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

    /// <summary>
    /// Result of InsertData: either a fresh store or a dedup match against an existing entry.
    /// </summary>
    public readonly struct InsertResult
    {
        public ulong BucketId { get; init; }
        public ulong BucketIndex { get; init; }
        public bool WasDeduplicated { get; init; }
        public float Similarity { get; init; }
        public string? MatchedStorageGuid { get; init; }
    }

    public async Task<InsertResult> InsertData(M_Data _data, ulong _id)
    {
        if (_data == null) throw new ArgumentNullException(nameof(_data));

        TouchAccess();

        if (_data.chunk == null || _data.chunk.Length == 0)
        {
            AddData(_data);
            string fallbackName = RocksDbBucketStorage.UlongToBitstring(ID);
            (ulong bid, ulong bidx) = await NetworkFileStorageHandler.StoreVector(fallbackName, _data);
            _data.id = bid;
            _data.index = bidx;
            return new InsertResult { BucketId = bid, BucketIndex = bidx };
        }

        await _storeSemaphore.WaitAsync();
        try
        {
            var chunkKey = HashChunk(_data.chunk);

            // ── Exact dedup: byte-identical chunk already in this bucket ──
            if (_seenChunks.TryGetValue(chunkKey, out var existing))
            {
                _data.storageGuid = chunkKey;
                _data.id = existing.Item1;
                _data.index = existing.Item2;
                return new InsertResult
                {
                    BucketId = existing.Item1,
                    BucketIndex = existing.Item2,
                    WasDeduplicated = true,
                    Similarity = 1.0f,
                    MatchedStorageGuid = chunkKey
                };
            }

            // ── Similarity dedup: catches within-file matches stored between search and store ──
            if (_data.vector != null && _data.vector.Length > 0)
            {
                float threshold = Globals.StoreSimilarityThreshold;
                var snapshot = GetDataSnapshot();
                float queryNormSq = _data.normSquared > 0f
                    ? _data.normSquared
                    : Misc.ComputeNormSquared(_data.vector);

                float bestSim = -1f;
                M_Data? bestMatch = null;

                // Build dense arrays of valid candidates for batch SIMD
                var validEntries = new M_Data[snapshot.Length];
                var candVecs = new float[snapshot.Length][];
                var candNorms = new float[snapshot.Length];
                int validCount = 0;
                for (int i = 0; i < snapshot.Length; i++)
                {
                    var entry = snapshot[i];
                    if (entry?.vector != null && entry.vector.Length == _data.vector.Length)
                    {
                        validEntries[validCount] = entry;
                        candVecs[validCount] = entry.vector;
                        candNorms[validCount] = entry.normSquared > 0f
                            ? entry.normSquared
                            : Misc.ComputeNormSquared(entry.vector);
                        validCount++;
                    }
                }

                if (validCount > 0)
                {
                    var sims = new float[validCount];

                    if (validCount >= 256)
                    {
                        int coreCount = Math.Max(2, Environment.ProcessorCount / 2);
                        int chunk = (validCount + coreCount - 1) / coreCount;
                        Parallel.ForEach(
                            Partitioner.Create(0, validCount, chunk),
                            new ParallelOptions { MaxDegreeOfParallelism = coreCount },
                            (range, _) =>
                            {
                                int len = range.Item2 - range.Item1;
                                var subVecs = new float[len][];
                                var subNorms = new float[len];
                                Array.Copy(candVecs, range.Item1, subVecs, 0, len);
                                Array.Copy(candNorms, range.Item1, subNorms, 0, len);
                                var subResults = new float[len];
                                Misc.BatchCosineSimilarity(_data.vector, queryNormSq, subVecs, subNorms, subResults, len);
                                Array.Copy(subResults, 0, sims, range.Item1, len);
                            });
                    }
                    else
                    {
                        Misc.BatchCosineSimilarity(_data.vector, queryNormSq, candVecs, candNorms, sims, validCount);
                    }

                    for (int i = 0; i < validCount; i++)
                    {
                        if (sims[i] >= threshold && sims[i] > bestSim)
                        {
                            bestSim = sims[i];
                            bestMatch = validEntries[i];
                        }
                    }
                }

                if (bestMatch != null)
                {
                    _data.storageGuid = bestMatch.storageGuid;
                    _data.id = bestMatch.id;
                    _data.index = bestMatch.index;
                    return new InsertResult
                    {
                        BucketId = bestMatch.id,
                        BucketIndex = bestMatch.index,
                        WasDeduplicated = true,
                        Similarity = bestSim,
                        MatchedStorageGuid = bestMatch.storageGuid
                    };
                }
            }

            // ── No match — store fresh ──
            string bucketName = RocksDbBucketStorage.UlongToBitstring(ID);
            (ulong bucketId, ulong bucketIndex) = await NetworkFileStorageHandler.StoreVector(bucketName, _data);

            _data.id = bucketId;
            _data.index = bucketIndex;

            _data.chunk = null;

            AddData(_data);
            _seenChunks.TryAdd(chunkKey, (bucketId, bucketIndex));

            return new InsertResult { BucketId = bucketId, BucketIndex = bucketIndex };
        }
        finally
        {
            _storeSemaphore.Release();
        }
    }

    // SearchData is no longer called in the hot path (SearchVector.Get uses ProcessSingleQuery).
    // Kept for backward compatibility.
    public Task<List<M_SearchResult>> SearchData(float[] _vector, float _minimum_similarity, int _k, int _i)
    {
        var results = new List<M_SearchResult>();
        var snapshot = GetDataSnapshot();
        float queryNormSq = Misc.ComputeNormSquared(_vector);

        float bestSim = -1f;
        M_Data? bestData = null;

        for (int idx = 0; idx < snapshot.Length; idx++)
        {
            var row = snapshot[idx];
            if (row?.vector == null || row.vector.Length != _vector.Length) continue;
            float cNorm = row.normSquared > 0f ? row.normSquared : Misc.ComputeNormSquared(row.vector);
            float sim = Misc.CalculateDistanceWithNorm(_vector, queryNormSq, row.vector, cNorm);
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
