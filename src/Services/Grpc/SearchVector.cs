
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Numerics;
using System.Text.Json;
using Agent.Modules.Agneta;
using Agent.Modules.Peer;
using Agent.Modules.Storage;
using Agent.Utils.Globals;
using Agent.Utils.Misc;
using Agent.Utils;
using Google.Protobuf;
using Google.Protobuf.Collections;
using Grpc.Core;
using Grpc.Net.Client;

public class SearchVectorService : SearchVector.SearchVectorBase
{
    /// <summary>
    /// Unified search: gather ALL vectors from ALL requested buckets (in-memory + single Postgres batch),
    /// then run a single cosine similarity pass over the combined set.
    /// </summary>
    public override async Task<SearchVector_Result> Get(SearchVector_Req request, ServerCallContext context)
    {
        using var rootSpan = Observability.StartStage("SearchVector.Get");
        try
        {
            var queryVector = request.Vector.ToArray();
            var allBitstrings = request.Bitstrings.ToList();

            if (allBitstrings.Count == 0)
            {
                return MakeSaveResult(request.Index);
            }

            // ── DHT range check (distributed mode only) ──
            if (!LocalModeDetector.IsLocalMode())
            {
                if (Globals._NODE.successor == null)
                    return MakeSaveResult(request.Index);

                bool inRange = Agent.Utils.Misc.Misc.IsKeyInRange(
                    Globals._NODE.id, Globals._NODE.successor.id, allBitstrings[0]);
                if (!inRange)
                    return await ClientGet(request, Globals._NODE.successor.ip);
            }

            // ──────────────────────────────────────────────────────────────
            // Step 1: Collect vectors from all buckets.
            //   a) In-memory buckets (already loaded) — free
            //   b) Remaining buckets — single batch Postgres query
            // ──────────────────────────────────────────────────────────────
            var gatherSw = Stopwatch.StartNew();

            // Lightweight struct to avoid allocations
            var candidates = new List<(float[] vector, string storageGuid, ulong bucketId, ulong bucketIndex)>();
            var bucketsToFetch = new List<string>();

            foreach (var bs in allBitstrings)
            {
                if (Globals._NODE.Buckets.TryGetValue(bs, out var bucket))
                {
                    // Fast path: bucket already in memory
                    foreach (var d in bucket.data)
                    {
                        if (d?.vector != null && d.vector.Length == queryVector.Length)
                            candidates.Add((d.vector, d.storageGuid, d.id, d.index));
                    }
                }
                else
                {
                    bucketsToFetch.Add(bs);
                }
            }

            // Single Postgres round-trip for all remaining buckets
            if (bucketsToFetch.Count > 0)
            {
                var dbRows = await NetworkFileStorageHandler.GetVectorsByBucketsAsync(bucketsToFetch);
                foreach (var (vec, sg, bid, bidx) in dbRows)
                {
                    if (vec != null && vec.Length == queryVector.Length)
                        candidates.Add((vec, sg, (ulong)bid, (ulong)bidx));
                }
            }

            gatherSw.Stop();
            Observability.RecordStage("GatherVectors", gatherSw.Elapsed.TotalMilliseconds,
                ("in_memory_buckets", allBitstrings.Count - bucketsToFetch.Count),
                ("pg_buckets", bucketsToFetch.Count),
                ("total_vectors", candidates.Count));

            if (candidates.Count == 0)
                return MakeSaveResult(request.Index);

            // ──────────────────────────────────────────────────────────────
            // Step 2: Single cosine similarity pass over ALL candidates.
            //   Parallel scan with thread-local top-K to minimise locking.
            // ──────────────────────────────────────────────────────────────
            var simSw = Stopwatch.StartNew();
            int k = Math.Max(1, request.K);
            float threshold = request.MinimumSimilarity;

            // Thread-local best candidates → merge at end
            var globalBest = new List<(int idx, float sim)>();
            var lockObj = new object();

            Parallel.ForEach(
                Partitioner.Create(0, candidates.Count),
                new ParallelOptions { MaxDegreeOfParallelism = Math.Max(1, (int)(Environment.ProcessorCount * 0.75)) },
                () => new List<(int idx, float sim)>(),  // thread-local
                (range, _, localBest) =>
                {
                    for (int i = range.Item1; i < range.Item2; i++)
                    {
                        var vec = candidates[i].vector;
                        // Quick NaN guard
                        bool bad = false;
                        for (int vi = 0; vi < vec.Length; vi++)
                        {
                            if (float.IsNaN(vec[vi]) || float.IsInfinity(vec[vi]))
                            { bad = true; break; }
                        }
                        if (bad) continue;

                        float sim = Misc.CalculateDistance(queryVector, vec);
                        if (sim >= threshold)
                        {
                            if (localBest.Count < k)
                            {
                                localBest.Add((i, sim));
                            }
                            else
                            {
                                // Replace worst
                                int worstIdx = 0;
                                float worstSim = localBest[0].sim;
                                for (int j = 1; j < localBest.Count; j++)
                                {
                                    if (localBest[j].sim < worstSim)
                                    { worstSim = localBest[j].sim; worstIdx = j; }
                                }
                                if (sim > worstSim)
                                    localBest[worstIdx] = (i, sim);
                            }
                        }
                    }
                    return localBest;
                },
                localBest =>
                {
                    if (localBest.Count == 0) return;
                    lock (lockObj)
                    {
                        foreach (var c in localBest)
                        {
                            if (globalBest.Count < k)
                            {
                                globalBest.Add(c);
                            }
                            else
                            {
                                int worstIdx = 0;
                                float worstSim = globalBest[0].sim;
                                for (int j = 1; j < globalBest.Count; j++)
                                {
                                    if (globalBest[j].sim < worstSim)
                                    { worstSim = globalBest[j].sim; worstIdx = j; }
                                }
                                if (c.sim > worstSim)
                                    globalBest[worstIdx] = c;
                            }
                        }
                    }
                }
            );

            simSw.Stop();
            Observability.RecordStage("CosineSimilarity", simSw.Elapsed.TotalMilliseconds,
                ("candidates_scanned", candidates.Count),
                ("matches_found", globalBest.Count));

            if (globalBest.Count == 0)
                return MakeSaveResult(request.Index);

            // ──────────────────────────────────────────────────────────────
            // Step 3: Fetch chunk bytes for top-K results (lazy — only for matches).
            // ──────────────────────────────────────────────────────────────
            var fetchSw = Stopwatch.StartNew();
            var sortedBest = globalBest.OrderByDescending(b => b.sim).ToList();
            var res = new SearchVector_Result { Save = false };

            var fetchTasks = sortedBest.Select(async b =>
            {
                var c = candidates[b.idx];
                byte[]? chunk = await ChunkCacheHandler.GetChunkAsync(c.storageGuid);
                if (chunk != null && chunk.Length > 0)
                {
                    return new SearchVectorObject
                    {
                        BucketId = c.bucketId,
                        BucketKey = (long)c.bucketIndex,
                        Similarity = b.sim,
                        Chunk = ByteString.CopyFrom(chunk),
                        Index = request.Index
                    };
                }
                return null;
            });

            var fetchedResults = await Task.WhenAll(fetchTasks);
            foreach (var r in fetchedResults)
            {
                if (r != null)
                    res.Results.Add(r);
            }
            fetchSw.Stop();
            Observability.RecordStage("FetchChunks", fetchSw.Elapsed.TotalMilliseconds,
                ("fetched", res.Results.Count));

            if (res.Results.Count > 0)
                return res;

            // All chunk fetches failed — fall back to save
            return MakeSaveResult(request.Index);
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
        catch(RpcException ex)
        {
            await AgnetaHandler.Log(2, $"gRPC error: {ex.Status.StatusCode} - {ex.Status.Detail}");
            throw;
        }
        catch (Exception ex)
        {
            await AgnetaHandler.Log(2, $"[SearchVector] General error: {ex.Message}");
            throw;
        }
    }
}
