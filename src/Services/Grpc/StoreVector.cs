
using System.Text.Json;
using Agent.Modules.Peer;
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

        if (allZeros)
        {
            reason = "all-zero";
            return true;
        }

        if (normSq <= 1e-12)
        {
            reason = $"near-zero-norm({normSq:E3})";
            return true;
        }

        return false;
    }

    public override async Task<StoreVector_Res> Store(StoreVector_Req request, ServerCallContext context)
    {
        Console.WriteLine($"Storing vector - Chunk size: {request.Chunk?.Length ?? 0}, Vector size: {request.Vector?.Count ?? 0}");
        
        if (request.Chunk == null || request.Chunk.Length == 0)
        {
            Console.WriteLine($"[ERROR] StoreVector: Received null or empty chunk from Gateway!");
            throw new ArgumentException("Chunk data cannot be null or empty", nameof(request));
        }
        
        if (request.Vector == null || request.Vector.Count == 0)
        {
            Console.WriteLine($"[ERROR] StoreVector: Received null or empty vector from Gateway!");
            throw new ArgumentException("Vector data cannot be null or empty", nameof(request));
        }

        if (IsInvalidVector(request.Vector, out string invalidReason))
        {
            Console.WriteLine($"[ERROR] StoreVector: Invalid vector ({invalidReason}) for bucket {request.Bitstring}; chunk size={request.Chunk.Length}");
            throw new ArgumentException($"Invalid vector: {invalidReason}", nameof(request));
        }
        
        M_Data _data = new M_Data();
        _data.vector = request.Vector.ToArray();
        _data.chunk = request.Chunk.ToArray();
        
        Console.WriteLine($"StoreVector: Prepared M_Data - Chunk size: {_data.chunk?.Length ?? 0}, Vector size: {_data.vector?.Length ?? 0}");
        
        // Store synchronously - wait for storage to complete
        // This ensures chunks are actually stored before returning
        try
        {
            (ulong _id, ulong _index) = await NodeService.StoreInBucket(Globals._NODE, request.Bitstring, _data, request.HeadRouteID);
            Console.WriteLine($"[StoreVector] ✅ Storage complete: id={_id}, index={_index}, chunk size={_data.chunk.Length} bytes");
            return new StoreVector_Res() { Id = _id, Index = _index };
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[StoreVector] ❌ Storage FAILED: {ex.Message}");
            Console.WriteLine($"[StoreVector] Stack trace: {ex.StackTrace}");
            throw; // Re-throw to let Gateway know storage failed
        }
    }
}
