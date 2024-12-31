
using Agent.Utils.Globals;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Grpc.Net.Client;

public class GetPredecessorService : GetPredecessor.GetPredecessorBase
{
    public override async Task<GetPredecessor_Result> Get(Empty request, ServerCallContext context)
    {
        GetPredecessor_Result res = new GetPredecessor_Result()
        {
            Id = Globals._NODE.predecessor.id,
            Ip = Globals._NODE.predecessor.ip
        };
        return res;
    }

    public async Task<GetPredecessor_Result> ClientGet(string _ip)
    {
        var channel = GrpcChannel.ForAddress(_ip);
        GetPredecessor.GetPredecessorClient _client = new GetPredecessor.GetPredecessorClient(channel);

        var response = await _client.GetAsync(new Empty());

        return response;
    }
}
