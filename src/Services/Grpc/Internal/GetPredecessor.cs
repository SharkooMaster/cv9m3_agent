
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
            Id = Globals._NODE.predecessor.id ? Globals._NODE.id,
            Ip = Globals._NODE.predecessor.ip ? Globals._NODE.ip
        };
        return res;
    }

    public async Task<GetPredecessor_Result> ClientGet(string _ip)
    {
        try
        {
            Console.WriteLine("Getting predecessor");
            var channel = GrpcChannel.ForAddress($"http://{_ip}:5000");
            GetPredecessor.GetPredecessorClient _client = new GetPredecessor.GetPredecessorClient(channel);

            await AgnetaHandler.Log(1, "Sending");
            var response = await _client.GetAsync(new Empty());
            await AgnetaHandler.Log(1, "Sent");

            return response;
        }
        catch (RpcException ex)
        {
            Console.WriteLine($"gRPC error: {ex.Status.StatusCode} - {ex.Status.Detail}");
            throw;
        }
        catch(Exception ex)
        {
            Console.WriteLine($"[GetPredecessor] General error: {ex.Message}");
            throw;
        }
    }
}
