
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
        Console.WriteLine("Storing vector");
        M_Data _data = new M_Data();
        _data.vector = request.Vector.ToArray();
        _data.chunk = request.Chunk.ToArray();
        // ulong _id = await NodeService.StoreInBucket(Globals._NODE, request.Bitstring, _data, request.HeadRouteID);
        (ulong _id, ulong _index) = await NodeService.StoreInBucket(Globals._NODE, request.Bitstring, _data, request.HeadRouteID);
        return new StoreVector_Res() { Id = _id, Index = _index };
    }
}
