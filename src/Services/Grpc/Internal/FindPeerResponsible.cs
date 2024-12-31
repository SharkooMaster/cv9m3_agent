
using Grpc.Core;
using Grpc.Net.Client;
using Agent.Modules.Peer;
using Agent.Utils.Globals;

public class FindPeerResponsibleService : FindPeerResponsible.FindPeerResponsibleBase
{
    public override async Task<QueryRes> Find(QueryReq request, ServerCallContext context)
    {
        string peerFound = await NodeService.FindPeerResponsible(Globals._NODE, request.Val);
        QueryRes resp = new QueryRes();
        resp.Res = peerFound;

        return resp;
    }

    public async Task<QueryRes> ClientFind(QueryReq request, string _ip)
    {
        Console.WriteLine("Sending grpc request, ClientFind (FindPeerResponsibleService)");
        try
        {
            var channel = GrpcChannel.ForAddress($"http://{_ip}:5000");
            FindPeerResponsible.FindPeerResponsibleClient _client = new FindPeerResponsible.FindPeerResponsibleClient(channel);

            var response = await _client.FindAsync(request);

            return response;
        }
        catch (RpcException ex)
        {
            Console.WriteLine($"gRPC error: {ex.Status.StatusCode} - {ex.Status.Detail}");
            throw;
        }
        catch(Exception ex)
        {
            Console.WriteLine($"General error: {ex.Message}");
            throw;
        }
    }
}
