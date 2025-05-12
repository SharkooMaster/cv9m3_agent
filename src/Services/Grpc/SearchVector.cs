
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
        await Parallel.ForAsync(0, request.Reqs.Count, options, async (i, ct) => 
        {
            (List<M_SearchResult>, bool, bool) query_res = await NodeService.SearchAll(
                Globals._NODE,
                request.Reqs[i].Bitstring,
                request.Reqs[i].Vector.ToArray(),
                request.Reqs[i].MinimumSimilarity,
                request.Reqs[i].K,
                request.Reqs[i],
                context
            );

            if(query_res.Item2 == true)
            {
                // Route to proper agent
                outgoingReqs.Add(request.Reqs[i]);
            }
            else
            {
                SearchVector_Result res = new SearchVector_Result();
                foreach (var item in query_res.Item1)
                {
                    res.Results.Add(new SearchVectorObject() {
                        SimilarityRate = item.similarity,
                        Chunk = ByteString.CopyFrom(item.chunk),
                        Id = Convert.ToUInt64(request.Reqs[i].Bitstring, 2),
                        Index = request.Reqs[i].Index
                    });
                }
                res.TargetIp = Misc.GetLocalIPAddress();
                res.Save = query_res.Item3;
                resultBag.Add(res);
            }
        });
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
