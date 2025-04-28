
using Agent.Models;
using Agent.Modules.Peer;
using Agent.Utils;
using Agent.Utils.Globals;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Grpc.Net.Client;

public class UpdateFingerTableService : UpdateFingerTable.UpdateFingerTableBase
{
    public override async Task<Empty> Update(UpdateFingerTable_Req request, ServerCallContext context)
    {
        M_Node new_node = new M_Node() { id=request.Id, ip=request.Ip };
        //Globals._NODE = await NodeService.UpdateFingerTable(Globals._NODE, new_node, request.FingerIndex);

        return new Empty();
    }

    public async Task ClientUpdate(UpdateFingerTable_Req req, string _ip, CancellationToken ct = default)
    {
        try
        {
            var _client = GrpcChannelFactory.GetClient(target: _ip, ctor: chan => new UpdateFingerTable.UpdateFingerTableClient(chan), roundRobin: false);

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
            Console.WriteLine($"[UpdateFingerTable] General error: {ex.Message}");
            throw;
        }
    }
}
