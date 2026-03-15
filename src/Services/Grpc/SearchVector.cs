
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Numerics;
using Agent.Modules.Agneta;
using Agent.Modules.Peer;
using Agent.Modules.Storage;
using Agent.Services.Cache;
using Agent.Utils.Globals;
using Agent.Utils.Misc;
using Agent.Utils;
using Agent.Services.Storage;
using Google.Protobuf;
using Grpc.Core;

public class SearchVectorService : SearchVector.SearchVectorBase
{
    // ──────────────────────────────────────────────────────────────────
    // BatchGet: process ALL queries for this agent in ONE gRPC call.
    // Gateway groups queries by agent (rendezvous hash) and sends a single
    // batch per agent. This eliminates ~595 round trips per file.
    // ──────────────────────────────────────────────────────────────────
    public override async Task<BatchSearchVector_Result> BatchGet(BatchSearchVector_Req request, ServerCallContext context)
    {
        var result = new BatchSearchVector_Result();
        var queries = request.Queries;
        if (queries.Count == 0)
            return result;

        int maxPar = Math.Max(4, Environment.ProcessorCount * 2);
        var results = new SearchVector_Result[queries.Count];

        await Parallel.ForEachAsync(
            Enumerable.Range(0, queries.Count),
            new ParallelOptions { MaxDegreeOfParallelism = maxPar },
            async (i, _) =>
            {
                results[i] = await ProcessSingleQuery(queries[i]);
            });

        result.Results.AddRange(results);
        return result;
    }

    /// <summary>
    /// Single-query search: hot buckets from RAM, cold buckets via direct RocksDB iterator.
    /// Hot path: in-memory SIMD scan (unchanged behavior for active buckets).
    /// Cold path: SearchBucketsDirect — prefix bloom skips irrelevant SSTs, inline cosine
    ///            similarity on the iterator, zero M_Data/M_Bucket allocation.
    /// </summary>
    private static async Task<SearchVector_Result> ProcessSingleQuery(SearchVector_Req request)
    {
        var queryVector = request.Vector.ToArray();
        int vecLen = queryVector.Length;

        if (request.Bitstrings.Count == 0)
            return MakeSaveResult(request.Index);

        float threshold = request.MinimumSimilarity;
        int requestedK = Math.Max(1, request.K);
        float queryNormSq = Misc.ComputeNormSquared(queryVector);

        // ── L1 bypass: route all buckets directly through RocksDB ──
        if (!BucketCacheManager.L1Enabled)
        {
            var allBucketIds = new List<ulong>(request.Bitstrings.Count);
            foreach (var bs in request.Bitstrings)
                allBucketIds.Add(RocksDbBucketStorage.BitstringToUlong(bs));

            var bucketStorage = BucketCacheManager.GetBucketStorage();
            if (bucketStorage != null && allBucketIds.Count > 0)
            {
                var (directBucketId, directIndex, directGuid, directSim) =
                    bucketStorage.SearchBucketsDirect(
                        allBucketIds, queryVector, queryNormSq, threshold, 4096);

                if (directSim >= threshold && directGuid != null)
                {
                    ByteString chunkBytes = ByteString.Empty;
                    var raw = await ChunkCacheHandler.GetChunkAsync(directGuid);
                    if (raw != null && raw.Length > 0)
                        chunkBytes = ByteString.CopyFrom(raw);

                    var res = new SearchVector_Result { Save = false };
                    res.Results.Add(new SearchVectorObject
                    {
                        BucketId = directBucketId,
                        BucketKey = (long)directIndex,
                        Similarity = directSim,
                        Chunk = chunkBytes,
                        Index = request.Index,
                        StorageGuid = directGuid
                    });
                    return res;
                }
            }
            return MakeSaveResult(request.Index);
        }

        // ── Phase 1: Collect candidates from hot (RAM) buckets ──
        const int MaxCandidates = 4096;
        var candidates = new List<(float[] vector, ulong bucketId, ulong bucketIndex, string? storageGuid, float normSq)>(256);
        var coldBucketIds = new List<ulong>();

        foreach (var bs in request.Bitstrings)
        {
            ulong bucketKey = RocksDbBucketStorage.BitstringToUlong(bs);

            if (Globals._NODE.Buckets.TryGetValue(bucketKey, out var bucket))
            {
                bucket.TouchAccess();
                var items = bucket.GetDataSnapshot();
                for (int j = 0; j < items.Length; j++)
                {
                    var d = items[j];
                    if (d?.vector != null && d.vector.Length == vecLen)
                        candidates.Add((d.vector, d.id, d.index, d.storageGuid, d.normSquared));
                }
            }
            else
            {
                coldBucketIds.Add(bucketKey);
            }

            if (candidates.Count >= MaxCandidates)
                break;
        }

        // ── Phase 2: Find best from hot candidates using batch SIMD ──
        float bestSim = -1f;
        ulong bestBucketId = 0, bestBucketIndex = 0;
        string? bestGuid = null;
        bool foundInRam = false;

        int count = candidates.Count;
        if (count > 0)
        {
            var similarities = new float[count];

            if (count >= 16)
            {
                var candVecs = new float[count][];
                var candNorms = new float[count];
                for (int i = 0; i < count; i++)
                {
                    candVecs[i] = candidates[i].vector;
                    candNorms[i] = candidates[i].normSq;
                }

                if (count >= 128)
                {
                    int coreCount = Math.Max(4, Environment.ProcessorCount);
                    int chunkSize = (count + coreCount - 1) / coreCount;
                    Parallel.ForEach(
                        Partitioner.Create(0, count, chunkSize),
                        new ParallelOptions { MaxDegreeOfParallelism = coreCount },
                        (range, _) =>
                        {
                            int len = range.Item2 - range.Item1;
                            var subVecs = new float[len][];
                            var subNorms = new float[len];
                            Array.Copy(candVecs, range.Item1, subVecs, 0, len);
                            Array.Copy(candNorms, range.Item1, subNorms, 0, len);
                            var subResults = new float[len];
                            Misc.BatchCosineSimilarity(queryVector, queryNormSq, subVecs, subNorms, subResults, len);
                            Array.Copy(subResults, 0, similarities, range.Item1, len);
                        });
                }
                else
                {
                    Misc.BatchCosineSimilarity(queryVector, queryNormSq, candVecs, candNorms, similarities, count);
                }
            }
            else
            {
                for (int i = 0; i < count; i++)
                    similarities[i] = Misc.CalculateDistanceWithNorm(queryVector, queryNormSq, candidates[i].vector, candidates[i].normSq);
            }

            for (int i = 0; i < count; i++)
            {
                if (similarities[i] >= threshold && similarities[i] > bestSim)
                {
                    bestSim = similarities[i];
                    var c = candidates[i];
                    bestBucketId = c.bucketId;
                    bestBucketIndex = c.bucketIndex;
                    bestGuid = c.storageGuid;
                    foundInRam = true;
                }
            }

            // K>1 path (high-entropy): return top-K from RAM candidates
            if (requestedK > 1)
            {
                var indices = new int[count];
                for (int i = 0; i < count; i++) indices[i] = i;
                Array.Sort(similarities, indices);
                Array.Reverse(similarities);
                Array.Reverse(indices);

                int returnCount = Math.Min(requestedK, count);
                var res = new SearchVector_Result { Save = bestSim < threshold };
                for (int i = 0; i < returnCount; i++)
                {
                    if (similarities[i] < threshold) break;
                    var c = candidates[indices[i]];
                    ByteString chunkBytes = ByteString.Empty;
                    if (!string.IsNullOrWhiteSpace(c.storageGuid))
                    {
                        var raw = await ChunkCacheHandler.GetChunkAsync(c.storageGuid);
                        if (raw != null && raw.Length > 0)
                            chunkBytes = ByteString.CopyFrom(raw);
                    }
                    res.Results.Add(new SearchVectorObject
                    {
                        BucketId = c.bucketId,
                        BucketKey = (long)c.bucketIndex,
                        Similarity = similarities[i],
                        Chunk = chunkBytes,
                        Index = request.Index,
                        StorageGuid = c.storageGuid ?? ""
                    });
                }
                if (res.Results.Count > 0) return res;
                return MakeSaveResult(request.Index);
            }
        }

        // ── Phase 3: Search cold (evicted) buckets directly on RocksDB ──
        if (coldBucketIds.Count > 0)
        {
            int remainingBudget = MaxCandidates - count;
            if (remainingBudget > 0)
            {
                var bucketStorage = BucketCacheManager.GetBucketStorage();
                if (bucketStorage != null)
                {
                    var (coldBucketId, coldIndex, coldGuid, coldSim) =
                        bucketStorage.SearchBucketsDirect(
                            coldBucketIds, queryVector, queryNormSq, threshold, remainingBudget);

                    if (coldSim > bestSim && coldSim >= threshold)
                    {
                        bestSim = coldSim;
                        bestBucketId = coldBucketId;
                        bestBucketIndex = coldIndex;
                        bestGuid = coldGuid;
                    }
                }
            }
        }

        // ── Return best match or save-new ──
        if (bestSim >= threshold && bestGuid != null)
        {
            ByteString chunkBytes = ByteString.Empty;
            var raw = await ChunkCacheHandler.GetChunkAsync(bestGuid);
            if (raw != null && raw.Length > 0)
                chunkBytes = ByteString.CopyFrom(raw);

            var res = new SearchVector_Result { Save = false };
            res.Results.Add(new SearchVectorObject
            {
                BucketId = bestBucketId,
                BucketKey = (long)bestBucketIndex,
                Similarity = bestSim,
                Chunk = chunkBytes,
                Index = request.Index,
                StorageGuid = bestGuid
            });
            return res;
        }

        return MakeSaveResult(request.Index);
    }

    /// <summary>
    /// Legacy single-query endpoint. Delegates to ProcessSingleQuery.
    /// Kept for backward compatibility but BatchGet is preferred.
    /// </summary>
    public override async Task<SearchVector_Result> Get(SearchVector_Req request, ServerCallContext context)
    {
        return await ProcessSingleQuery(request);
    }

    private static SearchVector_Result MakeSaveResult(int index)
    {
        var r = new SearchVector_Result { Save = true };
        r.Results.Add(new SearchVectorObject
        {
            BucketId = 0,
            BucketKey = 0,
            Similarity = 0,
            Chunk = ByteString.Empty,
            Index = index
        });
        return r;
    }

    public async Task<SearchVector_Result> ClientGet(SearchVector_Req req, string _ip, CancellationToken ct = default)
    {
        try
        {
            var _client = GrpcChannelFactory.GetClient(target: _ip, ctor: chan => new SearchVector.SearchVectorClient(chan), roundRobin: false);
            var deadline = DateTime.UtcNow.AddSeconds(Globals.GRPC_TIMEOUT);
            return await _client.GetAsync(req, deadline: deadline, cancellationToken: ct);
        }
        catch (RpcException) { throw; }
        catch (Exception ex)
        {
            throw new RpcException(new Status(StatusCode.Internal, $"[SearchVector] {ex.Message}"));
        }
    }
}
