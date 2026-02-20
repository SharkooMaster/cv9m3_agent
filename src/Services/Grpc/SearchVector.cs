
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Numerics;
using Agent.Modules.Agneta;
using Agent.Modules.Peer;
using Agent.Modules.Storage;
using Agent.Utils.Globals;
using Agent.Utils.Misc;
using Agent.Utils;
using Google.Protobuf;
using Grpc.Core;

public class SearchVectorService : SearchVector.SearchVectorBase
{
    /// <summary>
    /// Pure RAM search: gather ALL vectors from ALL requested buckets (in-memory only),
    /// then run a single cosine similarity pass over the combined set.
    /// No RocksDB reads. No Redis. No Postgres. No cold imports.
    /// Buckets are pre-loaded at startup via WarmUpBuckets().
    /// New buckets are added to RAM during StoreVector (via M_Bucket.InsertData).
    /// </summary>
    public override async Task<SearchVector_Result> Get(SearchVector_Req request, ServerCallContext context)
    {
        using var rootSpan = Observability.StartStage("SearchVector.Get");
        try
        {
            var queryVector = request.Vector.ToArray();
            var allBitstrings = request.Bitstrings.ToList();

            if (allBitstrings.Count == 0)
                return MakeSaveResult(request.Index);

            // ── NO DHT range check. Gateway already routed via rendezvous hash. ──
            // Agent accepts all requests. If bucket isn't here, it doesn't exist on this agent.

            // ──────────────────────────────────────────────────────────────
            // Step 1: Collect vectors from in-memory buckets ONLY (zero I/O)
            // ──────────────────────────────────────────────────────────────
            var candidates = new List<(float[] vector, string storageGuid, ulong bucketId, ulong bucketIndex)>();

            foreach (var bs in allBitstrings)
            {
                if (Globals._NODE.Buckets.TryGetValue(bs, out var bucket))
                {
                    // Fast path: bucket in RAM — iterate snapshot
                    foreach (var d in bucket.data)
                    {
                        if (d?.vector != null && d.vector.Length == queryVector.Length
                            && !string.IsNullOrEmpty(d.storageGuid))
                            candidates.Add((d.vector, d.storageGuid, d.id, d.index));
                    }
                }
                // else: bucket doesn't exist on this agent — skip (gateway will handle NeedToStore)
            }

            if (candidates.Count == 0)
                return MakeSaveResult(request.Index);

            // ──────────────────────────────────────────────────────────────
            // Step 2: Single cosine similarity pass over ALL candidates.
            // ──────────────────────────────────────────────────────────────
            int k = Math.Max(1, request.K);
            float threshold = request.MinimumSimilarity;

            // For small candidate sets (< 256), sequential is faster than Parallel overhead
            (int idx, float sim) best = (-1, -1f);

            if (candidates.Count < 256)
            {
                for (int i = 0; i < candidates.Count; i++)
                {
                    float sim = Misc.CalculateDistance(queryVector, candidates[i].vector);
                    if (sim >= threshold && sim > best.sim)
                        best = (i, sim);
                }
            }
            else
            {
                // Parallel scan for large candidate sets — unlimited parallelism
                (int idx, float sim) globalBest = (-1, -1f);
                var lockObj = new object();

                Parallel.ForEach(
                    Partitioner.Create(0, candidates.Count),
                    new ParallelOptions { MaxDegreeOfParallelism = -1 },
                    () => (-1, -1f),
                    (range, _, localBest) =>
                    {
                        for (int i = range.Item1; i < range.Item2; i++)
                        {
                            float sim = Misc.CalculateDistance(queryVector, candidates[i].vector);
                            if (sim >= threshold && sim > localBest.Item2)
                                localBest = (i, sim);
                        }
                        return localBest;
                    },
                    localBest =>
                    {
                        if (localBest.Item1 < 0) return;
                        lock (lockObj)
                        {
                            if (localBest.Item2 > globalBest.sim)
                                globalBest = localBest;
                        }
                    }
                );
                best = globalBest;
            }

            if (best.idx < 0)
                return MakeSaveResult(request.Index);

            // ──────────────────────────────────────────────────────────────
            // Step 3: Return the best match.
            //   Fetch chunk bytes from local cache/RocksDB only.
            //   If not locally available, return empty — gateway handles it.
            // ──────────────────────────────────────────────────────────────
            var c = candidates[best.idx];
            byte[]? chunk = await ChunkCacheHandler.GetChunkAsync(c.storageGuid);

            var res = new SearchVector_Result { Save = false };
            res.Results.Add(new SearchVectorObject
            {
                BucketId = c.bucketId,
                BucketKey = (long)c.bucketIndex,
                Similarity = best.sim,
                Chunk = (chunk != null && chunk.Length > 0) ? ByteString.CopyFrom(chunk) : ByteString.Empty,
                Index = request.Index
            });
            return res;
        }
        catch (Exception ex)
        {
            throw new RpcException(new Status(StatusCode.Internal, $"Agent search failed: {ex.Message}"));
        }
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
