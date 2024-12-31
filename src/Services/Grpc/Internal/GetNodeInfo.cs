
using Agent.Utils.Globals;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Grpc.Net.Client;

public class GetNodeInfoService : GetNodeInfo.GetNodeInfoBase
{
    public override async Task<GetNodeInfo_Result> Get(Empty request, ServerCallContext context)
    {
        GetNodeInfo_Result res = new GetNodeInfo_Result()
        {
            Ip = Globals._NODE.ip,
            Id = Globals._NODE.id
        };
        return res;
    }

    public async Task<GetNodeInfo_Result> ClientGet(string _ip)
    {
        var channel = GrpcChannel.ForAddress(_ip);
        GetNodeInfo.GetNodeInfoClient _client = new GetNodeInfo.GetNodeInfoClient(channel);

        var response = await _client.GetAsync(new Empty());

        return response;
    }
}
