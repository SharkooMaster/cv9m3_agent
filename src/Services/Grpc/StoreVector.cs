
using System.Text.Json;
using Agent.Modules.Peer;
using Agent.Utils;
using Agent.Utils.Globals;
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
    /// </summary>
    public override async Task<BatchStoreVector_Res> BatchStore(BatchStoreVector_Req request, ServerCallContext context)
    {
        var result = new BatchStoreVector_Res();
        if (request.Items.Count == 0)
            return result;

        // Process all store requests in parallel — each one hits RAM + RocksDB batch writer
        var results = new StoreVector_Res[request.Items.Count];

        // Cap parallelism to avoid unbounded memory growth during massive batch stores.
        // 2× CPU cores gives good throughput without starving the threadpool.
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
                    // Individual store failure: return (0, 0) — Cross handles this gracefully
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

    /// <summary>
    /// Core single-chunk store logic, shared by Store and BatchStore.
    /// </summary>
    private static async Task<StoreVector_Res> StoreSingle(StoreVector_Req request)
    {
        if (request.Chunk == null || request.Chunk.Length == 0)
            throw new ArgumentException("Chunk data cannot be null or empty", nameof(request));

        if (request.Vector == null || request.Vector.Count == 0)
            throw new ArgumentException("Vector data cannot be null or empty", nameof(request));

        if (IsInvalidVector(request.Vector, out string invalidReason))
            throw new ArgumentException($"Invalid vector: {invalidReason}", nameof(request));

        M_Data _data = new M_Data();
        _data.vector = request.Vector.ToArray();
        _data.chunk = request.Chunk.ToArray();

        (ulong _id, ulong _index) = await NodeService.StoreInBucket(
            Globals._NODE, request.Bitstring, _data, request.HeadRouteID);

        return new StoreVector_Res { Id = _id, Index = _index, StorageGuid = _data.storageGuid ?? "" };
    }
}
