
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
        M_Data _data = new M_Data();
        _data.vector = request.Vector.ToArray();
        _data.metadata = JsonDocument.Parse(request.Metadata).RootElement;
        ulong _id = await NodeService.StoreInBucket(Globals._NODE, request.Bitstring, _data);
        return new StoreVector_Res() { Id = _id };
    }
}
