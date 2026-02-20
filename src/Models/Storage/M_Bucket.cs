
using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Text;
using Agent.Utils;
using Agent.Utils.Misc;

public class M_Bucket
{
    public string ID { get; set; }
    public ulong lastId = 0; // Possibly needs atomic operations to avoid collision
    public ConcurrentBag<M_Data> data = new ConcurrentBag<M_Data>();

    // ── Dedup guard: track which chunks (by SHA256 of content) are already in the bag. ──
    // Prevents duplicate entries from concurrent stores of the same chunk content.
    private readonly ConcurrentDictionary<string, (ulong id, ulong index)> _seenChunks = new();

    public M_Bucket(string _ID)
    {
        ID = _ID;
    }

    public async Task<ulong> BookId()
    {
        ulong to_return = lastId;
        Interlocked.Increment(ref lastId);
        return to_return;
    }

    private static string HashChunk(byte[] chunk)
    {
        using var sha = SHA256.Create();
        var hash = sha.ComputeHash(chunk);
        var sb = new StringBuilder(hash.Length * 2);
        foreach (var b in hash) sb.Append(b.ToString("x2"));
        return sb.ToString();
    }

    public async Task<(ulong, ulong)> InsertData(M_Data _data, ulong _id){
        // IMPORTANT: For correct compression references, storing must return the real (bucketId, vectorIndex).
        // The Gateway/Cross encoder relies on these values being stable.
        if (_data == null) throw new ArgumentNullException(nameof(_data));

        // ── Dedup: if identical chunk content is already in this bucket, return its reference. ──
        if (_data.chunk != null && _data.chunk.Length > 0)
        {
            var chunkKey = HashChunk(_data.chunk);
            if (_seenChunks.TryGetValue(chunkKey, out var existing))
                return existing;

            // Store to the configured storage backend (LocalFileStorageService / GCS / etc).
            (int bucketId, int bucketIndex) = await NetworkFileStorageHandler.StoreVector(ID, _data);

            _data.id = (ulong)bucketId;
            _data.index = (ulong)bucketIndex;

            // Add to in-memory bag for future similarity search.
            data.Add(_data);
            _seenChunks.TryAdd(chunkKey, ((ulong)bucketId, (ulong)bucketIndex));

            return ((ulong)bucketId, (ulong)bucketIndex);
        }

        // Fallback: no chunk data — store anyway (shouldn't happen but safety net).
        data.Add(_data);
        (int bid, int bidx) = await NetworkFileStorageHandler.StoreVector(ID, _data);
        _data.id = (ulong)bid;
        _data.index = (ulong)bidx;
        return ((ulong)bid, (ulong)bidx);
    }
    
    public async Task<List<M_SearchResult>> SearchData(float[] _vector, float _minimum_similarity, int _k, int _i)
    {
        ConcurrentBag<M_SearchResult> to_return = new ConcurrentBag<M_SearchResult>();

        var candidates = new List<(M_Data data, float similarity)>();
        
        // No cap: Use unlimited parallelism for cosine similarity calculations
        // System will naturally limit based on available CPU
        var parallelOptions = new ParallelOptions
        {
            MaxDegreeOfParallelism = -1 // -1 = unlimited, let system handle it
        };

        // Snapshot the concurrent bag — scan ALL vectors (no cap).
        var rows = data.ToArray();

        // Full cosine similarity against all candidates; keep only top-K >= threshold.
        Parallel.ForEach(
            rows,
            parallelOptions,
            () => new List<(M_Data data, float similarity)>(),
            (row, state, local) =>
            {
                // Guard against bad/corrupt vector rows so one bad entry doesn't break a whole query.
                if (row?.vector == null || row.vector.Length != _vector.Length)
                {
                    return local;
                }
                bool invalid = false;
                for (int vi = 0; vi < row.vector.Length; vi++)
                {
                    float x = row.vector[vi];
                    if (float.IsNaN(x) || float.IsInfinity(x))
                    {
                        invalid = true;
                        break;
                    }
                }
                if (invalid)
                {
                    return local;
                }

                float similarity = Misc.CalculateDistance(_vector, row.vector);
                if (similarity >= _minimum_similarity)
                {
                    if (local.Count < _k)
                    {
                        local.Add((row, similarity));
                    }
                    else
                    {
                        // Replace worst local candidate if this is better (keeps local list bounded).
                        int worstIdx = 0;
                        float worstSim = local[0].similarity;
                        for (int j = 1; j < local.Count; j++)
                        {
                            if (local[j].similarity < worstSim)
                            {
                                worstSim = local[j].similarity;
                                worstIdx = j;
                            }
                        }

                        if (similarity > worstSim)
                        {
                            local[worstIdx] = (row, similarity);
                        }
                    }
                }

                return local;
            },
            local =>
            {
                if (local.Count == 0) return;
                lock (candidates)
                {
                    foreach (var c in local)
                    {
                        if (candidates.Count < _k)
                        {
                            candidates.Add(c);
                        }
                        else
                        {
                            // Replace worst global candidate if this is better.
                            int worstIdx = 0;
                            float worstSim = candidates[0].similarity;
                            for (int j = 1; j < candidates.Count; j++)
                            {
                                if (candidates[j].similarity < worstSim)
                                {
                                    worstSim = candidates[j].similarity;
                                    worstIdx = j;
                                }
                            }

                            if (c.similarity > worstSim)
                            {
                                candidates[worstIdx] = c;
                            }
                        }
                    }
                }
            }
        );

        // Second pass: Load chunks for top candidates (lazy loading from cache/disk)
        var tasks = candidates.OrderByDescending(c => c.similarity)
            .Take(_k)
            .Select(async candidate =>
            {
                byte[]? chunk = candidate.data.chunk;
                
                // If chunk not in memory, load from cache/disk
                if (chunk == null || chunk.Length == 0)
                {
                    if (!string.IsNullOrEmpty(candidate.data.storageGuid))
                    {
                        chunk = await Agent.Modules.Storage.ChunkCacheHandler.GetChunkAsync(candidate.data.storageGuid);
                        if (chunk != null)
                        {
                            // Update in-memory reference for future use
                            candidate.data.chunk = chunk;
                        }
                    }
                }

                if (chunk != null && chunk.Length > 0)
                {
                    return new M_SearchResult()
                    {
                        id = candidate.data.id,
                        index = candidate.data.index,
                        similarity = candidate.similarity,
                        chunk = chunk,
                        i = _i
                    };
                }
                else
                {
                    return null;
                }
            });

        var results = await Task.WhenAll(tasks);
        foreach (var result in results)
        {
            if (result != null)
            {
                to_return.Add(result);
            }
        }

        return to_return.OrderByDescending(r => r.similarity).ToList();
    }

}
