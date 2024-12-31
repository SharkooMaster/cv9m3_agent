
using Agent.Utils.Globals;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Grpc.Net.Client;

public class UpdatePredecessorService : UpdatePredecessor.UpdatePredecessorBase
{
    public override async Task<Empty> Update(UpdatePredecessor_Req request, ServerCallContext context)
    {
        Globals._NODE.predecessor.id = request.Id;
        Globals._NODE.predecessor.ip = request.Ip;
        return new Empty();
    }

    public async Task ClientUpdate(UpdatePredecessor_Req req, string _ip)
    {
        var channel = GrpcChannel.ForAddress(_ip);
        UpdatePredecessor.UpdatePredecessorClient _client = new UpdatePredecessor.UpdatePredecessorClient(channel);

        await _client.UpdateAsync(req);
    }
}
