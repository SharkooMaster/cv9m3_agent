
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text.Json;
using Agent.Modules.Agneta;
using Agent.Modules.Peer;
using Agent.Utils.Globals;
using Agent.Utils.Misc;
using Agent.Utils;
using Google.Protobuf;
using Google.Protobuf.Collections;
using Grpc.Core;
using Grpc.Net.Client;
using Xunit.Sdk;

public class SearchVectorService : SearchVector.SearchVectorBase
{
    private static int GetSearchBucketConcurrencyCap()
    {
        var raw = Environment.GetEnvironmentVariable("AGENT_SEARCH_BUCKET_CONCURRENCY");
        if (int.TryParse(raw, out var cap) && cap > 0)
            return cap;
        return 8;
    }

    /// <summary>Max neighbor buckets to search when the exact bucket has no match.
    /// Default 10 — the gateway already sorts neighbors by LSH confidence.</summary>
    private static int GetMaxNeighborSearch()
    {
        var raw = Environment.GetEnvironmentVariable("AGENT_MAX_NEIGHBOR_SEARCH");
        if (int.TryParse(raw, out var v) && v >= 0)
            return v;
        return 10;
    }

    public override async Task<SearchVector_Result> Get(SearchVector_Req request, ServerCallContext context)
    {
        using var rootSpan = Observability.StartStage("SearchVector.Get");
        try
        {
            var vector = request.Vector.ToArray();

            // ────────────────────────────────────────────────────────────
            // Phase 1 — Search the EXACT bucket (bitstring[0]) first.
            // This is the bucket the chunk's own LSH hash points to.
            // With a good hash, the match lives here ~99% of the time.
            // If we find a match ≥ threshold, return IMMEDIATELY —
            // skip all 64 neighbor buckets (and their Postgres cold-imports).
            // ────────────────────────────────────────────────────────────
            if (request.Bitstrings.Count > 0)
            {
                var phase1Sw = Stopwatch.StartNew();
                var (resultList, needsForward, bucketEmpty) = await NodeService.SearchAll(
                    Globals._NODE, /* canSave */ true,
                    request.Bitstrings[0], vector,
                    request.MinimumSimilarity, request.K,
                    request, context);
                phase1Sw.Stop();
                Observability.RecordStage("SearchExactBucket", phase1Sw.Elapsed.TotalMilliseconds,
                    ("result_count", resultList.Count), ("forward", needsForward));

                // Not in our range — forward to successor
                if (needsForward)
                    return await ClientGet(request, Globals._NODE.successor.ip);

                // Found results above threshold → done, skip neighbors
                if (!bucketEmpty && resultList.Count > 0)
                {
                    var res = new SearchVector_Result { Save = false };
                    foreach (var sr in resultList)
                    {
                        res.Results.Add(new SearchVectorObject
                        {
                            BucketId = sr.id,
                            BucketKey = (long)sr.index,
                            Similarity = sr.similarity,
                            Chunk = ByteString.CopyFrom(sr.chunk),
                            Index = request.Index
                        });
                    }
                    return res;
                }
            }

            // ────────────────────────────────────────────────────────────
            // Phase 2 — Exact bucket had no match.
            // Search a LIMITED number of neighbor buckets in parallel.
            // The gateway already sorts neighbors by LSH confidence
            // (least-confident bits first), so the first few are the
            // most likely to contain a match.
            // ────────────────────────────────────────────────────────────
            int maxNeighbors = GetMaxNeighborSearch();
            int neighborCount = Math.Min(request.Bitstrings.Count - 1, maxNeighbors);

            if (neighborCount > 0)
            {
                int concurrency = Math.Max(1, Math.Min(neighborCount, GetSearchBucketConcurrencyCap()));
                var parallelOptions = new ParallelOptions { MaxDegreeOfParallelism = concurrency };

                var neighborResults = new ConcurrentBag<M_SearchResult>();
                int foundMatch = 0; // 0 = false, 1 = true (for Interlocked)

                var phase2Sw = Stopwatch.StartNew();
                await Parallel.ForEachAsync(
                    Enumerable.Range(1, neighborCount),
                    parallelOptions,
                    async (i, ct) =>
                    {
                        // Early exit if another neighbor already found a match
                        if (Volatile.Read(ref foundMatch) == 1) return;

                        try
                        {
                            var (results, fwd, _) = await NodeService.SearchAll(
                                Globals._NODE, /* canSave */ false,
                                request.Bitstrings[i], vector,
                                request.MinimumSimilarity, request.K,
                                request, context);

                            if (results.Count > 0)
                            {
                                Interlocked.Exchange(ref foundMatch, 1);
                                foreach (var r in results)
                                    neighborResults.Add(r);
                            }
                        }
                        catch { /* swallow neighbor errors — exact bucket is authoritative */ }
                    });
                phase2Sw.Stop();
                Observability.RecordStage("SearchNeighborBuckets", phase2Sw.Elapsed.TotalMilliseconds,
                    ("neighbors_searched", neighborCount), ("match_found", foundMatch == 1));

                if (!neighborResults.IsEmpty)
                {
                    var best = neighborResults
                        .OrderByDescending(r => r.similarity)
                        .Take(request.K)
                        .ToList();

                    if (best.Count > 0)
                    {
                        var res = new SearchVector_Result { Save = false };
                        foreach (var sr in best)
                        {
                            res.Results.Add(new SearchVectorObject
                            {
                                BucketId = sr.id,
                                BucketKey = (long)sr.index,
                                Similarity = sr.similarity,
                                Chunk = ByteString.CopyFrom(sr.chunk),
                                Index = request.Index
                            });
                        }
                        return res;
                    }
                }
            }

            // ────────────────────────────────────────────────────────────
            // Phase 3 — No match found anywhere → tell gateway to store.
            // ────────────────────────────────────────────────────────────
            var saveRes = new SearchVector_Result { Save = true };
            saveRes.Results.Add(new SearchVectorObject
            {
                BucketId = 0,
                BucketKey = 0,
                Similarity = 0,
                Chunk = ByteString.Empty,
                Index = request.Index
            });
            return saveRes;
        }
        catch (Exception ex)
        {
            throw new RpcException(new Status(StatusCode.Internal, $"Agent search failed: {ex.Message}"));
        }
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
