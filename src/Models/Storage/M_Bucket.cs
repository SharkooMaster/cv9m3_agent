
using System.Collections.Concurrent;
using Agent.Utils;
using Agent.Utils.Misc;

public class M_Bucket
{
    public string ID { get; set; }
    public ulong lastId = 0; // Possibly needs atomic operations to avoid collision
    public ConcurrentBag<M_Data> data = new ConcurrentBag<M_Data>();

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

    public async Task<(ulong, ulong)> InsertData(M_Data _data, ulong _id){
        // IMPORTANT: For correct compression references, storing must return the real (bucketId, vectorIndex).
        // The Gateway/Cross encoder relies on these values being stable.
        if (_data == null) throw new ArgumentNullException(nameof(_data));

        // Ensure the in-memory bucket has the data for future similarity search.
        // (Chunk may be lazily loaded later, but we store it now.)
        data.Add(_data);

        // Store to the configured storage backend (LocalFileStorageService / GCS / etc).
        (int bucketId, int bucketIndex) = await NetworkFileStorageHandler.StoreVector(ID, _data);

        _data.id = (ulong)bucketId;
        _data.index = (ulong)bucketIndex;

        return ((ulong)bucketId, (ulong)bucketIndex);
    }
    
    public async Task<List<M_SearchResult>> SearchData(float[] _vector, float _minimum_similarity, int _k, int _i)
    {
        ConcurrentBag<M_SearchResult> to_return = new ConcurrentBag<M_SearchResult>();

        // IMPORTANT: No pre-filtering here. We want maximum recall on cosine >= _minimum_similarity.
        // Hamming-based gates can drop true positives and reduce compression quality.
        var candidates = new List<(M_Data data, float similarity)>();
        
        // DYNAMIC: Adjust parallelism based on current CPU and memory usage
        int baseParallelism = (int)(Environment.ProcessorCount * 0.75);
        int optimalParallelism = DynamicResourceManager.GetOptimalParallelism(baseParallelism);
        var parallelOptions = new ParallelOptions
        {
            MaxDegreeOfParallelism = Math.Max(1, optimalParallelism)
        };

        // Snapshot the concurrent bag for stable parallel iteration.
        var rows = data.ToArray();

        // Full cosine similarity against all candidates; keep only top-K >= threshold.
        Parallel.ForEach(
            rows,
            parallelOptions,
            () => new List<(M_Data data, float similarity)>(),
            (row, state, local) =>
            {
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
                    Console.WriteLine($"### WARNING ### Chunk not found for storageGuid: {candidate.data.storageGuid}");
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
