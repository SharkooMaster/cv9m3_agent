
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

        // All-zero and near-zero vectors can be legitimate for low-entropy chunks.
        // They should be handled by similarity math (CalculateDistance), not rejected here.
        if (allZeros || normSq <= 1e-12)
            return false;

        return false;
    }

    public override async Task<StoreVector_Res> Store(StoreVector_Req request, ServerCallContext context)
    {
        using var rootSpan = Observability.StartStage("StoreVector");
        var ingressSw = System.Diagnostics.Stopwatch.StartNew();
        
        if (request.Chunk == null || request.Chunk.Length == 0)
            throw new ArgumentException("Chunk data cannot be null or empty", nameof(request));
        
        if (request.Vector == null || request.Vector.Count == 0)
            throw new ArgumentException("Vector data cannot be null or empty", nameof(request));

        if (IsInvalidVector(request.Vector, out string invalidReason))
            throw new ArgumentException($"Invalid vector: {invalidReason}", nameof(request));
        
        M_Data _data = new M_Data();
        _data.vector = request.Vector.ToArray();
        _data.chunk = request.Chunk.ToArray();
        ingressSw.Stop();
        Observability.RecordStage("Deserialize", ingressSw.Elapsed.TotalMilliseconds, ("chunk_bytes", _data.chunk.Length));
        
        var storeSw = System.Diagnostics.Stopwatch.StartNew();
        (ulong _id, ulong _index) = await NodeService.StoreInBucket(Globals._NODE, request.Bitstring, _data, request.HeadRouteID);
        storeSw.Stop();
        Observability.RecordStage("Serialize", storeSw.Elapsed.TotalMilliseconds, ("stored", true));
        return new StoreVector_Res() { Id = _id, Index = _index };
    }
}
