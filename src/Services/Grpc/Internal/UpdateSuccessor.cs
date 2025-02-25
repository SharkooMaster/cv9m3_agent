
using Agent.Models;
using Agent.Utils.Globals;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Grpc.Net.Client;

public class UpdateSuccessorService : UpdateSuccessor.UpdateSuccessorBase
{
    public override async Task<Empty> Update(UpdateSuccessor_Req request, ServerCallContext context)
    {
        Globals._NODE.successor = new M_Node() { id = request.Id, ip = request.Ip };
        return new Empty();
    }

    public async Task ClientUpdate(UpdateSuccessor_Req req, string _ip)
    {
        try
        {
            var channel = GrpcChannel.ForAddress($"http://{_ip}:5000", Globals.GRPC_OPTIONS);
            UpdateSuccessor.UpdateSuccessorClient _client = new UpdateSuccessor.UpdateSuccessorClient(channel);

            await _client.UpdateAsync(req);
        }
        catch (RpcException ex)
        {
            Console.WriteLine($"gRPC error: {ex.Status.StatusCode} - {ex.Status.Detail}");
            throw;
        }
        catch(Exception ex)
        {
            Console.WriteLine($"[UpdateSuccessor] General error: {ex.Message}");
            throw;
        }
    }
}
