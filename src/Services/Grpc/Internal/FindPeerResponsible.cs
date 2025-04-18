
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

    public async Task<QueryRes> ClientFind(QueryReq request, string _ip, CancellationToken ct = default)
    {
        try
        {
            var channel = GrpcChannelFactory.GetChannel(_ip);
            FindPeerResponsible.FindPeerResponsibleClient _client = new FindPeerResponsible.FindPeerResponsibleClient(channel);

            var deadline = DateTime.UtcNow.AddSeconds(5);
            var response = await _client.FindAsync(request, deadline: deadline, cancellationToken: ct);

            return response;
        }
        catch (RpcException ex)
        {
            Console.WriteLine($"gRPC error: {ex.Status.StatusCode} - {ex.Status.Detail}");
            throw;
        }
        catch(Exception ex)
        {
            Console.WriteLine($"[FindPeerResponsible] General error: {ex.Message}");
            throw;
        }
    }
}
