
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
            Id = Globals._NODE.id,
            NodeName = Environment.GetEnvironmentVariable("MY_NODE_NAME") ?? ""
        };
        return res;
    }

    public async Task<GetNodeInfo_Result> ClientGet(string _ip, CancellationToken ct = default)
    {
        try
        {
            var _client = GrpcChannelFactory.GetClient(target: _ip, ctor: chan => new GetNodeInfo.GetNodeInfoClient(chan), roundRobin: false);

            var deadline = DateTime.UtcNow.AddSeconds(Globals.GRPC_TIMEOUT);
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
