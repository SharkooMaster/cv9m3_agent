
using System.Text.Json;
using Agent.Modules.Peer;
using Agent.Utils.Globals;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Newtonsoft.Json;

public class StoreVectorService : StoreVector.StoreVectorBase
{
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
        
        M_Data _data = new M_Data();
        _data.vector = request.Vector.ToArray();
        _data.chunk = request.Chunk.ToArray();
        
        Console.WriteLine($"StoreVector: Prepared M_Data - Chunk size: {_data.chunk?.Length ?? 0}, Vector size: {_data.vector?.Length ?? 0}");
        
        // ulong _id = await NodeService.StoreInBucket(Globals._NODE, request.Bitstring, _data, request.HeadRouteID);
        (ulong _id, ulong _index) = await NodeService.StoreInBucket(Globals._NODE, request.Bitstring, _data, request.HeadRouteID);
        return new StoreVector_Res() { Id = _id, Index = _index };
    }
}
