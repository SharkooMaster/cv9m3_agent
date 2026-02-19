
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
    /// Unified search: gather ALL vectors from ALL requested buckets (in-memory + single Postgres batch),
    /// then run a single cosine similarity pass over the combined set.
    /// Postgres results are cached in Globals._NODE.Buckets so repeat lookups never hit the DB.
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
            //   a) In-memory buckets (already loaded / cached) — zero cost
            //   b) Remaining buckets — single batch Postgres query
            //   c) Cache Postgres results in memory for future searches
            // ──────────────────────────────────────────────────────────────
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

                // Group by bucket name and import into in-memory cache
                // so future searches for these buckets skip Postgres entirely.
                var byBucket = new Dictionary<string, List<(float[] vec, string sg, long bid, long bidx)>>();
                foreach (var row in dbRows)
                {
                    if (row.vector != null && row.vector.Length == queryVector.Length)
                    {
                        candidates.Add((row.vector, row.storageGuid, (ulong)row.bucketId, (ulong)row.bucketIndex));

                        if (!byBucket.TryGetValue(row.bucketName, out var list))
                        {
                            list = new List<(float[], string, long, long)>();
                            byBucket[row.bucketName] = list;
                        }
                        list.Add((row.vector, row.storageGuid, row.bucketId, row.bucketIndex));
                    }
                }

                // Import into Globals._NODE.Buckets (idempotent — TryAdd ignores if another thread beat us)
                foreach (var (bName, rows) in byBucket)
                {
                    var newBucket = new M_Bucket(bName);
                    foreach (var (vec, sg, bid, bidx) in rows)
                    {
                        newBucket.data.Add(new M_Data
                        {
                            vector = vec,
                            storageGuid = sg,
                            id = (ulong)bid,
                            index = (ulong)bidx,
                            chunk = null // chunk bytes loaded lazily when needed
                        });
                    }
                    Globals._NODE.Buckets.TryAdd(bName, newBucket);
                }
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
                    var vec = candidates[i].vector;
                    float sim = Misc.CalculateDistance(queryVector, vec);
                    if (sim >= threshold && sim > best.sim)
                        best = (i, sim);
                }
            }
            else
            {
                // Parallel scan for large candidate sets
                (int idx, float sim) globalBest = (-1, -1f);
                var lockObj = new object();

                Parallel.ForEach(
                    Partitioner.Create(0, candidates.Count),
                    new ParallelOptions { MaxDegreeOfParallelism = Math.Max(1, (int)(Environment.ProcessorCount * 0.75)) },
                    () => (-1, -1f),  // thread-local best
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
