
using System.Text.Json;
using Agent.Modules.Peer;
using Agent.Modules.Storage;
using Agent.Services.Cache;
using Agent.Utils;
using Agent.Utils.Globals;
using Agent.Utils.Misc;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Newtonsoft.Json;

public class StoreVectorService : StoreVector.StoreVectorBase
{
    // Non-blocking store switch. When enabled, Store/BatchStore ack as soon as writes are
    // queued (in the write batcher + MRU cache); durability is provided by the background
    // 5s flush plus cross-side R/W replication (and/or a replicated PVC such as Ceph RBD).
    // We still drain synchronously when RocksDB reports write pressure, so a stalled engine
    // never gets piled on. Defaults to OFF → identical to the previous synchronous-flush
    // behavior, so durability is never weakened without an explicit opt-in.
    private static readonly bool StoreAsyncEnabled =
        (Environment.GetEnvironmentVariable("AGENT_STORE_ASYNC") ?? "").Trim().ToLowerInvariant()
            is "1" or "true" or "yes" or "on";

    /// <summary>
    /// Persist queued writes before acking, honoring the async-store switch.
    /// Sync mode (default): always flush — when this returns the data is on the WAL.
    /// Async mode: skip the flush on the fast path; only drain synchronously when the
    /// engine is under write pressure (which both relieves the stall and makes those
    /// writes durable). On the fast path durability comes from the background flush +
    /// replication, per the operator's opt-in.
    /// </summary>
    private static void PersistBeforeAck()
    {
        if (StoreAsyncEnabled && !NetworkFileStorageHandler.IsUnderWritePressure())
            return;

        try
        {
            NetworkFileStorageHandler.FlushPendingWrites();
        }
        catch (IOException ex)
        {
            throw new RpcException(new Status(StatusCode.Internal,
                $"RocksDB flush failed — data NOT persisted: {ex.Message}"));
        }
    }

    private static string Take(string s, int n) => s.Length <= n ? s : s.Substring(0, n);

    private static bool IsInvalidVector(IList<float> v, out string reason)
    {
        reason = string.Empty;
        if (v == null || v.Count == 0)
        {
            reason = "empty";
            return true;
        }
        if (v.Count != 64)
        {
            reason = $"dimension={v.Count}";
            return true;
        }

        double normSq = 0.0;
        bool allZeros = true;
        for (int i = 0; i < v.Count; i++)
        {
            float x = v[i];
            if (float.IsNaN(x) || float.IsInfinity(x))
            {
                reason = $"non-finite@{i}";
                return true;
            }
            if (x != 0f)
            {
                allZeros = false;
            }
            normSq += (double)x * x;
        }

        if (allZeros || normSq <= 1e-12)
            return false;

        return false;
    }

    /// <summary>
    /// Store a single chunk + vector. Kept for backward compat.
    /// </summary>
    public override async Task<StoreVector_Res> Store(StoreVector_Req request, ServerCallContext context)
    {
        var result = await StoreSingle(request);
        PersistBeforeAck();
        return result;
    }

    /// <summary>
    /// BatchStore: store ALL NeedToStore chunks for this agent in ONE gRPC call.
    /// Cross groups chunks by target agent (rendezvous hash) and sends one batch per agent.
    /// Eliminates ~8,000 per-chunk round trips per file → ~5 calls total.
    /// Per-bucket semaphores inside InsertData ensure concurrent stores to the
    /// same bucket are serialized (prevents dedup TOCTOU races) while stores to
    /// different buckets remain fully parallel.
    /// </summary>
    public override async Task<BatchStoreVector_Res> BatchStore(BatchStoreVector_Req request, ServerCallContext context)
    {
        var result = new BatchStoreVector_Res();
        if (request.Items.Count == 0)
            return result;

        var results = new StoreVector_Res[request.Items.Count];

        // Mirror the BatchGet sizing in SearchVector.BatchGet — the dedup
        // / bucket-allocate / write-record pipeline inside StoreSingle has
        // the same shape (RocksDB read for dedup, write batcher Put,
        // counter cache update) so the same parallelism budget applies.
        // ProcessorCount * 4 is the cap that keeps the L-flavor agents
        // CPU-bound rather than parallelism-throttled.
        int maxPar = Math.Max(4, Environment.ProcessorCount * 4);
        await Parallel.ForEachAsync(
            Enumerable.Range(0, request.Items.Count),
            new ParallelOptions { MaxDegreeOfParallelism = maxPar },
            async (i, ct) =>
            {
                try
                {
                    results[i] = await StoreSingle(request.Items[i]);
                }
                catch (Exception ex)
                {
                    // Per-item failure. Default Failed=false means success on the wire,
                    // so we must explicitly flip Failed=true on the failure path.
                    // Cross treats Failed=true rows as not-stored (skipped from the
                    // encode contract). Log loudly so operators can correlate.
                    Console.WriteLine(
                        $"[StoreVector] BatchStore item #{i} FAILED ({ex.GetType().Name}): {ex.Message}");
                    results[i] = new StoreVector_Res { Failed = true };
                }
            });

        PersistBeforeAck();

        result.Results.AddRange(results);
        return result;
    }

    private static async Task<StoreVector_Res> StoreSingle(StoreVector_Req request)
    {
        if (request.Chunk == null || request.Chunk.Length == 0)
            throw new ArgumentException("Chunk data cannot be null or empty", nameof(request));

        if (request.Vector == null || request.Vector.Count == 0)
            throw new ArgumentException("Vector data cannot be null or empty", nameof(request));

        if (IsInvalidVector(request.Vector, out string invalidReason))
            throw new ArgumentException($"Invalid vector: {invalidReason}", nameof(request));

        M_Data mdata = new M_Data();
        mdata.vector = request.Vector.ToArray();
        mdata.chunk = request.Chunk.ToArray();

        var insertResult = await NodeService.StoreInBucket(
            Globals._NODE, request.Bitstring, mdata, request.HeadRouteID);

        var res = new StoreVector_Res
        {
            Id = insertResult.BucketId,
            Index = insertResult.BucketIndex,
            StorageGuid = mdata.storageGuid ?? "",
            WasDeduplicated = insertResult.WasDeduplicated,
            Similarity = insertResult.Similarity,
            // Failed defaults to false on the wire → success/dedup. Only the catch path flips it true.
        };

        if (insertResult.WasDeduplicated && !string.IsNullOrWhiteSpace(insertResult.MatchedStorageGuid))
        {
            // Response invariant: when WasDeduplicated=true, sha256(BaseChunk) MUST
            // equal StorageGuid. Cross uses BaseChunk as the diff target for the
            // chunk it owns; storage_guid is what cross will fetch back at
            // decompress time. If we hand cross stale bytes paired with a fresh
            // storage_guid the resulting CCF cannot be decoded.
            //
            // The previous cache-only lookup silently returned BaseChunk=empty
            // when the matched chunk had been evicted from the bounded MRU
            // cache between search and store. Cross then retained its
            // search-phase Chunk (which hashes to a *different* GUID),
            // producing the 12% mismatch rate on
            // IntegrityCheck:StoreRoundTrip:DedupHit and silently corrupting
            // the encoded diff.
            //
            // Fall back to the disk-aware path. If even storage doesn't have
            // the matched chunk we fail the RPC so cross retries — that's the
            // honest signal (the chunk should be on disk because we stored it
            // before; if it isn't, something's wrong with that bucket).
            var baseBytes = ChunkCacheHandler.GetFromCacheOnly(insertResult.MatchedStorageGuid);
            if (baseBytes == null || baseBytes.Length == 0)
            {
                // Cache miss. Two possible causes:
                //   (a) MRU eviction since the base chunk was stored long ago.
                //   (b) Within-batch dedup against another chunk stored earlier in
                //       the SAME batch whose RocksDB write hasn't flushed yet.
                // Force a flush so case (b) becomes reachable from disk, then
                // do the async cache-or-disk lookup. The flush is idempotent
                // and cheap if nothing is pending.
                try { NetworkFileStorageHandler.FlushPendingWrites(); }
                catch { /* surface real errors via the read below */ }
                baseBytes = await ChunkCacheHandler.GetChunkAsync(insertResult.MatchedStorageGuid);
            }

            if (baseBytes != null && baseBytes.Length > 0)
            {
                res.BaseChunk = ByteString.CopyFrom(baseBytes);
            }
            else
            {
                Console.WriteLine(
                    $"[StoreVector] ERR: WasDeduplicated=true but base {Take(insertResult.MatchedStorageGuid, 16)} not in cache or storage; failing this row so cross retries.");
                throw new RpcException(new Status(StatusCode.DataLoss,
                    $"matched base {Take(insertResult.MatchedStorageGuid, 16)} unretrievable"));
            }
        }

        // ── Lane index: compute and store 64 mini-LSH entries for sub-chunk search ──
        if (!insertResult.WasDeduplicated && Globals.EnableLaneIndex)
        {
            try
            {
                var bucketStorage = BucketCacheManager.GetBucketStorage();
                if (bucketStorage != null)
                {
                    int subSize = Globals.MosaicSubChunkSize;
                    // Reuse the copy already made in mdata.chunk instead of materializing
                    // a second byte[] from the protobuf ByteString for every stored chunk.
                    var chunkBytes = mdata.chunk;
                    var laneHashes = Misc.ComputeLaneBitstrings(
                        chunkBytes, subSize, 64, Globals.LaneHashBits);
                    bucketStorage.StoreLaneEntries(
                        laneHashes, insertResult.BucketId, insertResult.BucketIndex,
                        mdata.storageGuid ?? "");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[StoreVector] Lane indexing failed (non-fatal): {ex.Message}");
            }
        }

        return res;
    }
}
