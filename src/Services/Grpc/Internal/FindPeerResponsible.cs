
using Grpc.Core;
using Grpc.Net.Client;
using Agent.Modules.Peer;
using Agent.Utils.Globals;

public class FindPeerResponsibleService : FindPeerResponsible.FindPeerResponsibleBase
{
    public override async Task<QueryRes> Find(QueryReq request, ServerCallContext context)
    {
        string peerFound = await NodeService.FindSuccessor(Globals._NODE, request.Val);
        QueryRes resp = new QueryRes();
        resp.Res = peerFound;

        return resp;
    }

    public async Task<QueryRes> ClientFind(QueryReq request, string _ip)
    {
        //Console.Writeline("Sending grpc request, ClientFind (FindPeerResponsibleService)");
        try
        {
            var channel = GrpcChannel.ForAddress($"http://{_ip}:5000", Globals.GRPC_OPTIONS);
            FindPeerResponsible.FindPeerResponsibleClient _client = new FindPeerResponsible.FindPeerResponsibleClient(channel);

            var response = await _client.FindAsync(request);

            return response;
        }
        catch (RpcException ex)
        {
            Console.Writeline($"gRPC error: {ex.Status.StatusCode} - {ex.Status.Detail}");
            throw;
        }
        catch(Exception ex)
        {
            Console.Writeline($"[FindPeerResponsible] General error: {ex.Message}");
            throw;
        }
    }
}
