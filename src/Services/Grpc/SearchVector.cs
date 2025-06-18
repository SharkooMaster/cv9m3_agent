
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text.Json;
using Agent.Modules.Agneta;
using Agent.Modules.Peer;
using Agent.Utils.Globals;
using Agent.Utils.Misc;
using Google.Protobuf;
using Google.Protobuf.Collections;
using Grpc.Core;
using Grpc.Net.Client;
using Xunit.Sdk;

public class SearchVectorService : SearchVector.SearchVectorBase
{
    public override async Task<SearchVector_Result> Get(SearchVector_Req request, ServerCallContext context)
    {
        // SearchAll(M_Node node, string _bitstring, float[] _vector, float _minimum_similarity, int _k, SearchVector_Req _req, ServerCallContext context)
        SearchVector_Result res = new SearchVector_Result();
        SearchVectorObject? SavedObject = null;

        bool forward = false;
        bool save = true;
        for (int i = 0; i < request.Bitstrings.Count; i++)
        {
            (List<M_SearchResult>, bool, bool) result = await NodeService.SearchAll
            (
                Globals._NODE, (i == 0), request.Bitstrings[i], request.Vector.ToArray(), request.MinimumSimilarity, request.K, request, context
            );

            if (result.Item2)
            {
                // forward
                forward = true;
                break;
            }

            // If a similare vector was found
            if (!result.Item3 && result.Item1.Count > 0)
            {
                save = false;

                // Insert
                for (int j = 0; j < result.Item1.Count; j++)
                {
                    M_SearchResult currentSearchResult = result.Item1[j];
                    res.Results.Add(new SearchVectorObject()
                    {
                        BucketId = currentSearchResult.id,
                        BucketKey = (long)currentSearchResult.index,
                        Similarity = currentSearchResult.similarity,
                        Chunk = ByteString.CopyFrom(currentSearchResult.chunk),
                        Index = request.Index
                    });
                }
            }

            if (result.Item3 && (i == 0)) // Original bucket, save
            {
                M_SearchResult currentSearchResult = result.Item1[0];
                SavedObject = new SearchVectorObject()
                {
                    BucketId = currentSearchResult.id,
                    BucketKey = (long)currentSearchResult.index,
                    Similarity = currentSearchResult.similarity,
                    Chunk = ByteString.CopyFrom(currentSearchResult.chunk),
                    Index = request.Index
                };
            }
        }

        if (forward)
        {
            return await ClientGet(request, Globals._NODE.successor.ip);
        }

        // If no results and save
        if (save)
        {
            res.Save = true;
            res.Results.Add(SavedObject);
        }

        return res;
    }

    public async Task<SearchVector_Result> ClientGet(SearchVector_Req req, string _ip, CancellationToken ct = default)
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
