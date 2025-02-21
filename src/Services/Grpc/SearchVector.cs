
using System.Text.Json;
using Agent.Modules.Agneta;
using Agent.Modules.Peer;
using Agent.Utils.Globals;
using Grpc.Core;
using Grpc.Net.Client;

public class SearchVectorService : SearchVector.SearchVectorBase
{
    public override async Task<SearchVector_Result> Get(SearchVector_Req request, ServerCallContext context)
    {
        Console.WriteLine("Request recieved");
        List<M_SearchResult> query_res = await NodeService.SearchAll(Globals._NODE, request.Bitstring, request.Vector.ToArray(), request.MinimumSimilarity, request.K, request);
        SearchVector_Result res = new SearchVector_Result();
        foreach (var item in query_res)
        {
            res.Results.Add(new SearchVectorObject() { SimilarityRate = item.similarity, Metadata = item.metadata.ToString() });
        }

        return res;
    }

    public async Task<SearchVector_Result> ClientGet(SearchVector_Req req, string _ip)
    {
        try
        {
            var channel = GrpcChannel.ForAddress($"http://{_ip}:5000");
            SearchVector.SearchVectorClient _client = new SearchVector.SearchVectorClient(channel);

            return await _client.GetAsync(req);
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
