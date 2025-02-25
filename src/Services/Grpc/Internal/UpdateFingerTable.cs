
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

    public async Task ClientUpdate(UpdateFingerTable_Req req, string _ip)
    {
        try
        {
            Console.WriteLine("Sending UpdateFingerTable request");
            var channel = GrpcChannel.ForAddress($"http://{_ip}:5000", Globals.GRPC_OPTIONS);
            UpdateFingerTable.UpdateFingerTableClient _client = new UpdateFingerTable.UpdateFingerTableClient(channel);

            await _client.UpdateAsync(req);
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
