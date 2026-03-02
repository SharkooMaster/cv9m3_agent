
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
    public override Task<BatchSearchVector_Result> BatchGet(BatchSearchVector_Req request, ServerCallContext context)
    {
        var result = new BatchSearchVector_Result();
        var queries = request.Queries;
        if (queries.Count == 0)
            return Task.FromResult(result);

        // Process all queries in parallel on this agent — pure RAM, no I/O
        // Cap parallelism to prevent threadpool starvation under heavy concurrent BatchGet requests.
        int maxPar = Math.Max(4, Environment.ProcessorCount * 2);
        var results = new SearchVector_Result[queries.Count];

        Parallel.For(0, queries.Count, new ParallelOptions { MaxDegreeOfParallelism = maxPar }, i =>
        {
            results[i] = ProcessSingleQuery(queries[i]);
        });

        result.Results.AddRange(results);
        return Task.FromResult(result);
    }

    /// <summary>
    /// Single-query search: pure RAM for cosine sim, O(1) cache read for matched chunk.
    /// Returns (bucketId, bucketIndex, similarity, chunk bytes) — eliminates the lazy fetch phase.
    /// Rendezvous hashing guarantees this agent owns the bucket, so the chunk is in our MRU cache.
    /// </summary>
    private static SearchVector_Result ProcessSingleQuery(SearchVector_Req request)
    {
        var queryVector = request.Vector.ToArray();
        int vecLen = queryVector.Length;

        if (request.Bitstrings.Count == 0)
            return MakeSaveResult(request.Index);

        // ── Collect candidates from L1 (RAM) then L2 (RocksDB) ──
        // L1 hit: pure RAM, zero I/O. L2 hit: loaded via prefix iterator (fast sequential scan).
        // Cap total candidates to prevent cosine similarity from growing unbounded
        // as buckets accumulate vectors over time.
        const int MaxCandidates = 4096;
        var candidates = new List<(float[] vector, ulong bucketId, ulong bucketIndex, string? storageGuid)>(256);

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
                    candidates.Add((d.vector, d.id, d.index, d.storageGuid));
            }

            // Cap total candidates to bound cosine similarity time
            if (candidates.Count >= MaxCandidates)
                break;
        }

        if (candidates.Count == 0)
            return MakeSaveResult(request.Index);

        // ── Cosine similarity pass ──
        float threshold = request.MinimumSimilarity;
        int requestedK = Math.Max(1, request.K);

        // Compute similarity for all candidates
        var scored = new (int idx, float sim)[candidates.Count];
        if (candidates.Count < 512)
        {
            for (int i = 0; i < candidates.Count; i++)
                scored[i] = (i, Misc.CalculateDistance(queryVector, candidates[i].vector));
        }
        else
        {
            Parallel.ForEach(
                Partitioner.Create(0, candidates.Count),
                new ParallelOptions { MaxDegreeOfParallelism = Math.Max(4, Environment.ProcessorCount) },
                (range, _) =>
                {
                    for (int i = range.Item1; i < range.Item2; i++)
                        scored[i] = (i, Misc.CalculateDistance(queryVector, candidates[i].vector));
                }
            );
        }

        // Find the single best above threshold (Level 1)
        int bestIdx = -1;
        float bestSim = -1f;
        for (int i = 0; i < scored.Length; i++)
        {
            if (scored[i].sim >= threshold && scored[i].sim > bestSim)
            {
                bestSim = scored[i].sim;
                bestIdx = scored[i].idx;
            }
        }

        // Level 1 match found — return single best (unchanged behavior)
        if (bestIdx >= 0)
        {
            var c = candidates[bestIdx];
            ByteString chunkBytes = ByteString.Empty;
            if (!string.IsNullOrWhiteSpace(c.storageGuid))
            {
                var raw = ChunkCacheHandler.GetFromCacheOnly(c.storageGuid);
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

        // No Level 1 match. If K > 1, return top-K candidates for mosaic consideration.
        if (requestedK > 1 && scored.Length > 0)
        {
            Array.Sort(scored, (a, b) => b.sim.CompareTo(a.sim));
            int returnCount = Math.Min(requestedK, scored.Length);

            var res = new SearchVector_Result { Save = true };
            for (int i = 0; i < returnCount; i++)
            {
                var c = candidates[scored[i].idx];
                ByteString chunkBytes = ByteString.Empty;
                if (!string.IsNullOrWhiteSpace(c.storageGuid))
                {
                    var raw = ChunkCacheHandler.GetFromCacheOnly(c.storageGuid);
                    if (raw != null && raw.Length > 0)
                        chunkBytes = ByteString.CopyFrom(raw);
                }
                res.Results.Add(new SearchVectorObject
                {
                    BucketId = c.bucketId,
                    BucketKey = (long)c.bucketIndex,
                    Similarity = scored[i].sim,
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
    public override Task<SearchVector_Result> Get(SearchVector_Req request, ServerCallContext context)
    {
        return Task.FromResult(ProcessSingleQuery(request));
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
