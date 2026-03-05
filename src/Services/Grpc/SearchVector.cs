
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
    /// Single-query search: pure RAM for cosine sim, O(1) cache read for matched chunk.
    /// Returns (bucketId, bucketIndex, similarity, chunk bytes) — eliminates the lazy fetch phase.
    /// Rendezvous hashing guarantees this agent owns the bucket, so the chunk is in our MRU cache.
    /// </summary>
    private static async Task<SearchVector_Result> ProcessSingleQuery(SearchVector_Req request)
    {
        var queryVector = request.Vector.ToArray();
        int vecLen = queryVector.Length;

        if (request.Bitstrings.Count == 0)
            return MakeSaveResult(request.Index);

        // ── Collect candidates from L1 (RAM) then L2 (RocksDB) ──
        const int MaxCandidates = 4096;
        var candidates = new List<(float[] vector, ulong bucketId, ulong bucketIndex, string? storageGuid, float normSq)>(256);

        foreach (var bs in request.Bitstrings)
        {
            ulong bucketKey = RocksDbBucketStorage.BitstringToUlong(bs);
            M_Bucket? bucket;
            if (!Globals._NODE.Buckets.TryGetValue(bucketKey, out bucket))
            {
                bucket = BucketCacheManager.LoadAndCache(bs);
                if (bucket == null || bucket.DataCount == 0) continue;
            }
            bucket.TouchAccess();

            var items = bucket.GetDataSnapshot();
            for (int j = 0; j < items.Length; j++)
            {
                var d = items[j];
                if (d?.vector != null && d.vector.Length == vecLen)
                    candidates.Add((d.vector, d.id, d.index, d.storageGuid, d.normSquared));
            }

            if (candidates.Count >= MaxCandidates)
                break;
        }

        if (candidates.Count == 0)
            return MakeSaveResult(request.Index);

        // ── Cosine similarity pass (batch SIMD with pre-computed norms) ──
        float threshold = request.MinimumSimilarity;
        int requestedK = Math.Max(1, request.K);
        int count = candidates.Count;
        float queryNormSq = Misc.ComputeNormSquared(queryVector);

        var similarities = new float[count];

        if (count >= 16)
        {
            // Extract arrays for batch processing
            var candVecs = new float[count][];
            var candNorms = new float[count];
            for (int i = 0; i < count; i++)
            {
                candVecs[i] = candidates[i].vector;
                candNorms[i] = candidates[i].normSq;
            }

            // Parallelize across CPU cores — threshold lowered to use idle cores
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

        // Find the single best above threshold (Level 1)
        int bestIdx = -1;
        float bestSim = -1f;
        for (int i = 0; i < count; i++)
        {
            if (similarities[i] >= threshold && similarities[i] > bestSim)
            {
                bestSim = similarities[i];
                bestIdx = i;
            }
        }

        bool hasL1 = bestIdx >= 0;

        // K=1 (standard path): return single L1 match or save-new
        if (requestedK <= 1)
        {
            if (hasL1)
            {
                var c = candidates[bestIdx];
                ByteString chunkBytes = ByteString.Empty;
                if (!string.IsNullOrWhiteSpace(c.storageGuid))
                {
                    var raw = await ChunkCacheHandler.GetChunkAsync(c.storageGuid);
                    if (raw != null && raw.Length > 0)
                        chunkBytes = ByteString.CopyFrom(raw);
                }
                var res = new SearchVector_Result { Save = false };
                res.Results.Add(new SearchVectorObject
                {
                    BucketId = c.bucketId,
                    BucketKey = (long)c.bucketIndex,
                    Similarity = bestSim,
                    Chunk = chunkBytes,
                    Index = request.Index,
                    StorageGuid = c.storageGuid ?? ""
                });
                return res;
            }
            return MakeSaveResult(request.Index);
        }

        // K>1 (high-entropy path): return top-K candidates sorted by similarity.
        if (count > 0)
        {
            var indices = new int[count];
            for (int i = 0; i < count; i++) indices[i] = i;
            Array.Sort(similarities, indices);
            // Sort put ascending — reverse to get descending
            Array.Reverse(similarities);
            Array.Reverse(indices);

            int returnCount = Math.Min(requestedK, count);
            var res = new SearchVector_Result { Save = !hasL1 };
            for (int i = 0; i < returnCount; i++)
            {
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
