
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
        try
        {
            NetworkFileStorageHandler.FlushPendingWrites();
        }
        catch (IOException ex)
        {
            throw new RpcException(new Status(StatusCode.Internal,
                $"RocksDB flush failed — data NOT persisted: {ex.Message}"));
        }
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

        int maxPar = Math.Max(4, Environment.ProcessorCount * 2);
        await Parallel.ForEachAsync(
            Enumerable.Range(0, request.Items.Count),
            new ParallelOptions { MaxDegreeOfParallelism = maxPar },
            async (i, ct) =>
            {
                try
                {
                    results[i] = await StoreSingle(request.Items[i]);
                }
                catch
                {
                    results[i] = new StoreVector_Res { Id = 0, Index = 0 };
                }
            });

        try
        {
            NetworkFileStorageHandler.FlushPendingWrites();
        }
        catch (IOException ex)
        {
            throw new RpcException(new Status(StatusCode.Internal,
                $"RocksDB flush failed — data NOT persisted: {ex.Message}"));
        }

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
            Similarity = insertResult.Similarity
        };

        if (insertResult.WasDeduplicated && !string.IsNullOrWhiteSpace(insertResult.MatchedStorageGuid))
        {
            var baseBytes = ChunkCacheHandler.GetFromCacheOnly(insertResult.MatchedStorageGuid);
            if (baseBytes != null && baseBytes.Length > 0)
                res.BaseChunk = ByteString.CopyFrom(baseBytes);
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
                    var laneHashes = Misc.ComputeLaneBitstrings(
                        request.Chunk.ToArray(), subSize, 64, Globals.LaneHashBits);
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
