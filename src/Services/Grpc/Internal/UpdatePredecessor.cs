
using Agent.Models;
using Agent.Utils.Globals;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Grpc.Net.Client;

public class UpdatePredecessorService : UpdatePredecessor.UpdatePredecessorBase
{
    public override async Task<Empty> Update(UpdatePredecessor_Req request, ServerCallContext context)
    {
        Globals._NODE.predecessor = new M_Node() { id = request.Id, ip = request.Ip };
        return new Empty();
    }

    public async Task ClientUpdate(UpdatePredecessor_Req req, string _ip, CancellationToken ct = default)
    {
        try
        {
            var _client = GrpcChannelFactory.GetClient(target: _ip, ctor: chan => new UpdatePredecessor.UpdatePredecessorClient(chan), roundRobin: false);

            var deadline = DateTime.UtcNow.AddSeconds(Globals.GRPC_TIMEOUT);
            await _client.UpdateAsync(req, deadline: deadline, cancellationToken: ct);
        }
        catch (RpcException ex)
        {
            Console.WriteLine($"gRPC error: {ex.Status.StatusCode} - {ex.Status.Detail}");
            throw;
        }
        catch(Exception ex)
        {
            Console.WriteLine($"[UpdatePredecessor] General error: {ex.Message}");
            throw;
        }
    }
}
