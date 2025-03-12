
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
        try
        {
            var channel = GrpcChannel.ForAddress($"http://{_ip}:80", Globals.GRPC_OPTIONS);
            GetNodeInfo.GetNodeInfoClient _client = new GetNodeInfo.GetNodeInfoClient(channel);

            var response = await _client.GetAsync(new Empty());

            return response;
        }
        catch (RpcException ex)
        {
            //Console.Writeline($"gRPC error: {ex.Status.StatusCode} - {ex.Status.Detail}");
            throw;
        }
        catch(Exception ex)
        {
            //Console.Writeline($"[GetNodeInfo] General error: {ex.Message}");
            throw;
        }
    }
}
