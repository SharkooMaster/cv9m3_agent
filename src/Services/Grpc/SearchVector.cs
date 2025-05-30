
using System.Collections.Concurrent;
using System.Diagnostics;
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
    public override async Task<SearchVector_Results> Get(SearchVector_Reqs request, ServerCallContext context)
    {
        //Console.WriteLine("Request recieved");
        SearchVector_Results ret = new SearchVector_Results();
        SearchVector_Reqs outgoingBatch = new SearchVector_Reqs();

        ConcurrentBag<SearchVector_Result> resultBag = new();
        ConcurrentBag<SearchVector_Req> outgoingReqs = new();

        Stopwatch sw = new Stopwatch();
        sw.Start();
        ParallelOptions options = new ();
        var searchTasks = request.Reqs.Select(async req =>
        {
            try
            {
                (List<M_SearchResult>, bool, bool) query_res = await NodeService.SearchAll(
                    Globals._NODE,
                    req.Bitstring,
                    req.Vector.ToArray(),
                    req.MinimumSimilarity,
                    req.K,
                    req,
                    context
                );

                if(query_res.Item2 == true)
                {
                    // Route to proper agent
                    outgoingReqs.Add(req);
                }
                else
                {
                    SearchVector_Result res = new SearchVector_Result();
                    foreach (var item in query_res.Item1)
                    {
                        if(item == null){ Console.WriteLine("Item is null"); }
                        if(req == null){ Console.WriteLine("req is null"); }
                        if(res == null){ Console.WriteLine("req is null"); }

                        res.Results.Add(new SearchVectorObject() {
                            SimilarityRate = item.similarity,
                            Chunk = ByteString.CopyFrom(item.chunk),
                            Id = item.id,
                            Index = item.Index
                        });
                    }
                    res.TargetIp = Misc.GetLocalIPAddress();
                    res.Save = query_res.Item3;
                    resultBag.Add(res);
                }
            }
            catch (System.Exception ex)
            {
                Console.WriteLine($"ERROR:SearchVector(Get):: {ex.Message} ; {ex.Data}");
                throw;
            }
        });
        await Task.WhenAll(searchTasks);
        ret.Results.AddRange(resultBag);
        outgoingBatch.Reqs.AddRange(outgoingReqs);
        sw.Stop();
        Console.WriteLine($"time to search: {sw.ElapsedMilliseconds}ms");

        if(outgoingBatch.Reqs.Count > 0)
        {
            SearchVector_Results outgoingRes = await ClientGet(outgoingBatch, Globals._NODE.successor.ip);
            ret.Results.AddRange(outgoingRes.Results);
        }

        return ret;
    }

    public async Task<SearchVector_Results> ClientGet(SearchVector_Reqs req, string _ip, CancellationToken ct = default)
    {
        try
        {
            var _client = GrpcChannelFactory.GetClient(target: _ip, ctor: chan => new SearchVector.SearchVectorClient(chan), roundRobin: false);

            var deadline = DateTime.UtcNow.AddSeconds(Globals.GRPC_TIMEOUT);
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
