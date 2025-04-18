
using Agent.Modules.Agneta;
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
            Ip = Globals._NODE.predecessor.ip,
        };
        return res;
    }

    public async Task<GetPredecessor_Result> ClientGet(string _ip, CancellationToken ct = default)
    {
        try
        {
            var _client = GrpcChannelFactory.GetClient(ip: _ip, chan => new GetPredecessor.GetPredecessorClient(chan));

            var deadline = DateTime.UtcNow.AddSeconds(5);
            return await _client.GetAsync(new Empty(), deadline: deadline, cancellationToken: ct);
        }
        catch (RpcException ex)
        {
            await AgnetaHandler.Log(2, $"gRPC error: {ex.Status.StatusCode} - {ex.Status.Detail}");
            throw;
        }
        catch(Exception ex)
        {
            await AgnetaHandler.Log(2, $"[GetPredecessor] General error: {ex.Message}");
            throw;
        }
    }
}
