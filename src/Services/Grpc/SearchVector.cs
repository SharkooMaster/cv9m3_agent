
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
        var results = new SearchVector_Result[queries.Count];

        Parallel.For(0, queries.Count, new ParallelOptions { MaxDegreeOfParallelism = -1 }, i =>
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

        // ── Collect candidates from in-memory buckets (zero I/O) ──
        // Carry storageGuid through so we can fetch the base chunk on match.
        var candidates = new List<(float[] vector, ulong bucketId, ulong bucketIndex, string? storageGuid)>(128);

        foreach (var bs in request.Bitstrings)
        {
            if (Globals._NODE.Buckets.TryGetValue(bs, out var bucket))
            {
                var items = bucket.GetDataSnapshot();
                for (int j = 0; j < items.Length; j++)
                {
                    var d = items[j];
                    if (d?.vector != null && d.vector.Length == vecLen)
                        candidates.Add((d.vector, d.id, d.index, d.storageGuid));
                }
            }
        }

        if (candidates.Count == 0)
            return MakeSaveResult(request.Index);

        // ── Single cosine similarity pass ──
        float threshold = request.MinimumSimilarity;
        int bestIdx = -1;
        float bestSim = -1f;

        if (candidates.Count < 512)
        {
            for (int i = 0; i < candidates.Count; i++)
            {
                float sim = Misc.CalculateDistance(queryVector, candidates[i].vector);
                if (sim >= threshold && sim > bestSim)
                {
                    bestSim = sim;
                    bestIdx = i;
                }
            }
        }
        else
        {
            int localBestIdx = -1;
            float localBestSim = -1f;
            var lockObj = new object();

            Parallel.ForEach(
                Partitioner.Create(0, candidates.Count),
                new ParallelOptions { MaxDegreeOfParallelism = -1 },
                () => (-1, -1f),
                (range, _, local) =>
                {
                    for (int i = range.Item1; i < range.Item2; i++)
                    {
                        float sim = Misc.CalculateDistance(queryVector, candidates[i].vector);
                        if (sim >= threshold && sim > local.Item2)
                            local = (i, sim);
                    }
                    return local;
                },
                local =>
                {
                    if (local.Item1 < 0) return;
                    lock (lockObj)
                    {
                        if (local.Item2 > localBestSim)
                        {
                            localBestSim = local.Item2;
                            localBestIdx = local.Item1;
                        }
                    }
                }
            );
            bestIdx = localBestIdx;
            bestSim = localBestSim;
        }

        if (bestIdx < 0)
            return MakeSaveResult(request.Index);

        // ── Return metadata + base chunk bytes ──
        // Rendezvous hashing guarantees we own this bucket, so the chunk is
        // in our MRU cache (O(1) sync read). This eliminates ~1000 separate
        // GetChunkByReference gRPC calls per file.
        var c = candidates[bestIdx];
        ByteString chunkBytes = ByteString.Empty;
        if (!string.IsNullOrWhiteSpace(c.storageGuid))
        {
            // O(1) sync read from MRU cache — no I/O, no blocking
            var raw = ChunkCacheHandler.GetFromCacheOnly(c.storageGuid);
            if (raw != null && raw.Length > 0)
            {
                chunkBytes = ByteString.CopyFrom(raw);
            }
            // If cache miss (rare — chunk evicted from 512MB MRU), Cross falls back to lazy fetch
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
