
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

    public async Task<GetNodeInfo_Result> ClientGet(string _ip, CancellationToken ct = default)
    {
        try
        {
            var channel = GrpcChannelFactory.GetChannel(_ip);
            GetNodeInfo.GetNodeInfoClient _client = new GetNodeInfo.GetNodeInfoClient(channel);

            var deadline = DateTime.UtcNow.AddSeconds(5);
            var response = await _client.GetAsync(new Empty(), deadline: deadline, cancellationToken: ct);
            return response;
        }
        catch (RpcException ex)
        {
            Console.WriteLine($"gRPC error: {ex.Status.StatusCode} - {ex.Status.Detail}");
            throw;
        }
        catch(Exception ex)
        {
            Console.WriteLine($"[GetNodeInfo] General error: {ex.Message}");
            throw;
        }
    }
}
