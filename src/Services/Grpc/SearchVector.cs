
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text.Json;
using Agent.Modules.Agneta;
using Agent.Modules.Peer;
using Agent.Utils.Globals;
using Agent.Utils.Misc;
using Agent.Utils;
using Google.Protobuf;
using Google.Protobuf.Collections;
using Grpc.Core;
using Grpc.Net.Client;
using Xunit.Sdk;

public class SearchVectorService : SearchVector.SearchVectorBase
{
    private static int GetSearchBucketConcurrencyCap()
    {
        var raw = Environment.GetEnvironmentVariable("AGENT_SEARCH_BUCKET_CONCURRENCY");
        if (int.TryParse(raw, out var cap) && cap > 0)
            return cap;
        return 8;
    }

    public override async Task<SearchVector_Result> Get(SearchVector_Req request, ServerCallContext context)
    {
        // SearchAll(M_Node node, string _bitstring, float[] _vector, float _minimum_similarity, int _k, SearchVector_Req _req, ServerCallContext context)
        SearchVector_Result res = new SearchVector_Result();
        SearchVectorObject? SavedObject = null;

        bool forward = false;
        bool save = true;
        
        // OPTIMIZED: Search buckets in parallel instead of sequentially for better performance
        // Use dynamic resource management to prevent CPU overload
        int baseParallelism = Math.Min(request.Bitstrings.Count, Environment.ProcessorCount);
        int optimalParallelism = DynamicResourceManager.GetOptimalParallelism(baseParallelism);
        optimalParallelism = Math.Min(optimalParallelism, GetSearchBucketConcurrencyCap());
        var parallelOptions = new ParallelOptions
        {
            MaxDegreeOfParallelism = Math.Max(1, optimalParallelism)
        };
        
        var searchResults = new ConcurrentBag<(int index, List<M_SearchResult> results, bool forward, bool isDuplicate, bool isOriginal)>();
        bool foundForward = false;
        var forwardLock = new object();
        
        // OPTIMIZATION: More aggressive early termination for faster compression
        // If we find a very good match (similarity >= 0.92) in original bucket, we can skip remaining buckets
        // Lower threshold from 0.98 to 0.92 for better performance while still maintaining quality
        const float EARLY_TERMINATION_THRESHOLD = 0.92f;
        bool foundExcellentMatch = false;
        var excellentMatchLock = new object();

        await Parallel.ForEachAsync(
            Enumerable.Range(0, request.Bitstrings.Count),
            parallelOptions,
            async (i, ct) =>
            {
                lock (forwardLock)
                {
                    if (foundForward) return; // Early exit if forwarding needed
                }

                // EARLY TERMINATION: If we found an excellent match in original bucket, skip remaining buckets
                if (i > 0) // Not the original bucket
                {
                    lock (excellentMatchLock)
                    {
                        if (foundExcellentMatch) return; // Skip if we already found excellent match
                    }
                }
                
                (List<M_SearchResult>, bool, bool) result = await NodeService.SearchAll
                (
                    Globals._NODE, (i == 0), request.Bitstrings[i], request.Vector.ToArray(), request.MinimumSimilarity, request.K, request, context
                );

                if (result.Item2)
                {
                    // forward - set flag and stop processing
                    lock (forwardLock)
                    {
                        foundForward = true;
                    }
                    searchResults.Add((i, result.Item1, true, result.Item3, i == 0));
                    return;
                }

                // OPTIMIZATION: Early termination - if original bucket has excellent match, skip remaining buckets
                // Threshold 0.92: Good balance between speed and compression quality
                if (i == 0 && result.Item1.Count > 0)
                {
                    float bestSimilarity = result.Item1.Max(r => r.similarity);
                    if (bestSimilarity >= EARLY_TERMINATION_THRESHOLD) // 0.92 threshold for faster compression
                    {
                        lock (excellentMatchLock)
                        {
                            foundExcellentMatch = true;
                        }
                        Console.WriteLine($"[SearchVector] Early termination: Found excellent match (similarity={bestSimilarity:F3}) in original bucket - skipping remaining buckets");
                    }
                }

                searchResults.Add((i, result.Item1, false, result.Item3, i == 0));
            }
        );

        // Process results (original bucket first, then others)
        var sortedResults = searchResults.OrderBy(r => r.index).ToList();
        
        foreach (var (index, resultList, needsForward, isDuplicateFlag, isOriginal) in sortedResults)
        {
            if (needsForward)
            {
                forward = true;
                break;
            }

            // If a similar vector was found (original logic: !result.Item3 && result.Item1.Count > 0)
            if (!isDuplicateFlag && resultList.Count > 0)
            {
                save = false;

                // Insert all results from this bucket
                foreach (var currentSearchResult in resultList)
                {
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

            if (isDuplicateFlag && isOriginal) // Original bucket, save (original logic: result.Item3 && (i == 0))
            {
                // If result.Item1 has items, use the first one (from previous storage)
                // Otherwise, SavedObject remains null - Gateway will store new chunk
                if (resultList.Count > 0)
                {
                    M_SearchResult currentSearchResult = resultList[0];
                    SavedObject = new SearchVectorObject()
                    {
                        BucketId = currentSearchResult.id,
                        BucketKey = (long)currentSearchResult.index,
                        Similarity = currentSearchResult.similarity,
                        Chunk = ByteString.CopyFrom(currentSearchResult.chunk),
                        Index = request.Index
                    };
                    Console.WriteLine($"[SearchVector] Found existing chunk for index {request.Index}");
                }
                else
                {
                    Console.WriteLine($"[SearchVector] New chunk needs to be saved for index {request.Index} - Gateway will provide chunk");
                }
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
            // Create a placeholder result - Gateway will store the actual chunk
            // The chunk will be provided by Gateway when it calls StoreVector
            if (SavedObject != null)
            {
                res.Results.Add(SavedObject);
            }
            else
            {
                // Create a minimal placeholder result
                res.Results.Add(new SearchVectorObject()
                {
                    BucketId = 0, // Will be set when stored
                    BucketKey = 0,
                    Similarity = 0,
                    Chunk = ByteString.Empty, // Empty - Gateway will provide actual chunk
                    Index = request.Index
                });
            }
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
