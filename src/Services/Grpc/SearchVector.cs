
using System.Text.Json;
using Agent.Modules.Agneta;
using Agent.Modules.Peer;
using Agent.Utils.Globals;
using Agent.Utils.Misc;
using Google.Protobuf;
using Grpc.Core;
using Grpc.Net.Client;

public class SearchVectorService : SearchVector.SearchVectorBase
{
    public override async Task<SearchVector_Result> Get(SearchVector_Req request, ServerCallContext context)
    {
        //Console.WriteLine("Request recieved");
        (List<M_SearchResult>, bool) query_res = await NodeService.SearchAll(
            Globals._NODE,
            request.Bitstring,
            request.Vector.ToArray(),
            request.MinimumSimilarity,
            request.K,
            request,
            context
        );
        SearchVector_Result res = new SearchVector_Result();
        if(query_res.Item2)
        {
            res.Forward = true;
            res.TargetIp = Globals._NODE.successor.ip;
        }
        //Console.WriteLine($"query_res length: {query_res.Count}");

        foreach (var item in query_res.Item1)
        {
            res.Results.Add(new SearchVectorObject() {
                SimilarityRate = item.similarity,
                Chunk = ByteString.CopyFrom(item.chunk),
                Id = Convert.ToUInt64(request.Bitstring, 2)
            });
        }
        res.TargetIp = Misc.GetLocalIPAddress();
        //Console.WriteLine($"res length: {res.Results.Count}");

        return res;
    }

    public async Task<SearchVector_Result> ClientGet(SearchVector_Req req, string _ip, CancellationToken ct = default)
    {
        try
        {
            var _client = GrpcChannelFactory.GetClient(target: _ip, ctor: chan => new SearchVector.SearchVectorClient(chan), roundRobin: false);

            var deadline = DateTime.UtcNow.AddSeconds(5);
            return await _client.GetAsync(req, deadline: deadline, cancellationToken: ct);
        }
        catch(RpcException ex)
        {
            await AgnetaHandler.Log(2, $"gRPC error: {ex.Status.StatusCode} - {ex.Status.Detail}");
            throw;
        }
        catch (Exception ex)
        {
            await AgnetaHandler.Log(2, $"[SearchVector] General error: {ex.Message}");
            throw;
        }
    }
}
