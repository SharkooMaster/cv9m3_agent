using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Text.Json;
using Agent.Interfaces;
using Agent.Models;
using Agent.Modules.Agneta;
using Agent.Utils;
using Agent.Utils.Globals;
using Agent.Utils.Misc;
using Agent.Services.Storage;
using Google.Api;
using Grpc.Core;
using Xunit.Sdk;

namespace Agent.Modules.Peer;

public static class NodeService
{
    public static int _nextFinger = 0;

    public static async Task<M_Node> JoinNetwork(M_Node node, string bootstrap_node)
    {
        GetNodeInfoService _getNodeInfoService = new GetNodeInfoService();
        GetPredecessorService _getPredecessorService = new GetPredecessorService();
        GetSuccessorService _getSuccessorService = new GetSuccessorService();
        GetHealthService _getHealth = new GetHealthService();

        UpdateSuccessorService _updateSuccessorService = new UpdateSuccessorService();
        UpdatePredecessorService _updatePredecessorService = new UpdatePredecessorService();

        if(bootstrap_node == null)
        {
            Console.WriteLine("Only node in the network");
            node.successor   = new M_Node() { id = node.id, ip = node.ip };
            node.predecessor = new M_Node() { id = node.id, ip = node.ip };

            Globals._NODE = node;
            return node;
        }

        // Get my successor
        string successor_ip = await S_FindPeerResponsible(node.id, bootstrap_node);
        GetNodeInfo_Result getSuccessor_res = await _getNodeInfoService.ClientGet(successor_ip);
        M_Node successor = new M_Node() { id = getSuccessor_res.Id, ip = getSuccessor_res.Ip };

        node.successor = successor;

        try
        {
            Console.WriteLine($"[JoinNetwork] Verifying chosen successor {node.successor.ip}...");

            GetPredecessorService _getPredecessorServiceTemp = new GetPredecessorService();
            var testResult = await _getPredecessorServiceTemp.ClientGet(node.successor.ip);

            if (testResult == null || string.IsNullOrWhiteSpace(testResult.Ip))
            {
                Console.WriteLine($"[JoinNetwork] Warning: Successor {node.successor.ip} verification failed (null). Retrying FindPeerResponsible...");

                // Try to find a better successor
                successor_ip = await S_FindPeerResponsible(node.id, bootstrap_node);
                getSuccessor_res = await _getNodeInfoService.ClientGet(successor_ip);
                node.successor = new M_Node() { id = getSuccessor_res.Id, ip = getSuccessor_res.Ip };

                Console.WriteLine($"[JoinNetwork] New successor selected: {node.successor.ip}");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[JoinNetwork] Error verifying successor {node.successor.ip}: {ex.Message}");
            // If successor dead, try finding a new one
            successor_ip = await S_FindPeerResponsible(node.id, bootstrap_node);
            getSuccessor_res = await _getNodeInfoService.ClientGet(successor_ip);
            node.successor = new M_Node() { id = getSuccessor_res.Id, ip = getSuccessor_res.Ip };

            Console.WriteLine($"[JoinNetwork] New successor selected after verification failure: {node.successor.ip}");
        }

        // Get my successors predecessor
        GetPredecessor_Result getPredecessor_res = await _getPredecessorService.ClientGet(node.successor.ip);
        M_Node predecessor = new M_Node() { id = getPredecessor_res.Id, ip = getPredecessor_res.Ip };

        node.predecessor = predecessor;

        // Notify my successor that im its new predecessor
        UpdatePredecessor_Req updatePredecessor_req = new UpdatePredecessor_Req() { Id = node.id, Ip = node.ip };
        await _updatePredecessorService.ClientUpdate(updatePredecessor_req, node.successor.ip);

        // Notify my predecessor that im its new successor
        UpdateSuccessor_Req updateSuccessor_req = new UpdateSuccessor_Req() { Id = node.id, Ip = node.ip };
        await _updateSuccessorService.ClientUpdate(updateSuccessor_req, node.predecessor.ip);

        Globals._NODE = node;
        return node;
    }
    
    private static async Task<string> S_FindPeerResponsible(ulong target, string _ip)
    {
        FindPeerResponsibleService _findPeerResponsible = new FindPeerResponsibleService();

        QueryReq req = new QueryReq() { Val = target };
        QueryRes res = await _findPeerResponsible.ClientFind(req, _ip);
        return res.Res;
    }

    public static async Task<string> FindSuccessor(M_Node node, ulong target)
    {
        if(node.predecessor != null && NodeUtils.inBetween(target, node.predecessor.id, node.id))
        {
            return node.ip;
        }
        else if(node.successor != null && NodeUtils.inBetween(target, node.id, node.successor.id))
        {
            return node.successor.ip;
        }
        else
        {
            if(target < node.id)
            {
                if (node.predecessor == null)
                    return node.ip; // Fallback to current node
                return await S_FindPeerResponsible(target, node.predecessor.ip);
            }
            else
            {
                return await S_FindPeerResponsible(target, node.successor.ip);
            }
            M_Node peer = await ClosestPreceedingNode(node, target);
            return await S_FindPeerResponsible(target, peer.ip);
        }
    }
    
    private static async Task<M_Node> ClosestPreceedingNode(M_Node node, ulong target)
    {
        ulong[] fingerTableKeys = node.fingerTable.Keys.ToArray();
        for (int i = Globals.FINGER_TABLE_SIZE - 1; i >= 0; i--)
        {
            if(NodeUtils.inBetween(node.fingerTable[fingerTableKeys[i]].id, node.id, target))
            {
                return node.fingerTable[fingerTableKeys[i]];
            }
        }
        return node;
    }

    public static async Task<M_Node> VerifySuccessor(M_Node node)
    {
        GetPredecessorService _getPredecessorService = new GetPredecessorService();
        UpdatePredecessorService _updatePredecessorService = new UpdatePredecessorService();

        try
        {
            if (string.IsNullOrWhiteSpace(node.successor?.ip))
            {
                Console.WriteLine("[VerifySuccessor] Warning: successor IP is null or empty.");
                return node;
            }

            // Add retry logic with exponential backoff
            const int maxRetries = 3;
            for (int attempt = 0; attempt < maxRetries; attempt++)
            {
                try
                {
                    // Create a cancellation token with a longer timeout
                    using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

                    GetPredecessor_Result getPredecessor_result = await _getPredecessorService.ClientGet(node.successor.ip, cts.Token);

                    if (getPredecessor_result == null)
                    {
                        Console.WriteLine($"[VerifySuccessor] Warning: No predecessor info received from {node.successor.ip}.");

                        // Wait before retrying
                        if (attempt < maxRetries - 1)
                        {
                            await Task.Delay(TimeSpan.FromSeconds(Math.Pow(2, attempt)));
                            continue;
                        }

                        return node;
                    }

                    // Rest of your logic...
                    if (getPredecessor_result.Id != node.id && getPredecessor_result.Ip != node.ip)
                    {
                        Console.WriteLine($"[VerifySuccessor] Updating successor to {getPredecessor_result.Ip}.");
                        // Your existing code...
                    }

                    // If we get here, we succeeded
                    return node;
                }
                catch (RpcException rpcEx) when (attempt < maxRetries - 1)
                {
                    Console.WriteLine($"[VerifySuccessor] Attempt {attempt + 1}/{maxRetries} failed: {rpcEx.Status}. Retrying...");
                    await Task.Delay(TimeSpan.FromSeconds(Math.Pow(2, attempt)));
                }
            }

            // All retries failed
            Console.WriteLine($"[VerifySuccessor] All {maxRetries} attempts to verify successor {node.successor.ip} failed.");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[VerifySuccessor] Exception while verifying successor: {ex.Message}");
        }

        return node;
    }

    /// <summary>
    /// Pure RAM search — NO cold imports, NO RocksDB reads, NO Redis.
    /// Buckets are pre-loaded at startup. Gateway routes via rendezvous hash.
    /// </summary>
    public static async Task<(List<M_SearchResult>, bool, bool)> 
      SearchAll(M_Node node, bool canSave, string _bitstring, float[] _vector, float _minimum_similarity, int _k, SearchVector_Req _req, ServerCallContext context)
    {
        using var rootSpan = Observability.StartStage("Node.SearchAll");

        ulong bucketKey = RocksDbBucketStorage.BitstringToUlong(_bitstring);

        // ── L1 bypass: search directly on RocksDB ──
        if (!Agent.Services.Cache.BucketCacheManager.L1Enabled)
        {
            var bucketStorage = Agent.Services.Cache.BucketCacheManager.GetBucketStorage();
            if (bucketStorage != null)
            {
                float queryNormSq = Agent.Utils.Misc.Misc.ComputeNormSquared(_vector);
                var searchSw = System.Diagnostics.Stopwatch.StartNew();
                var results = bucketStorage.SearchSingleBucketDirect(
                    bucketKey, _vector, queryNormSq, _minimum_similarity, _k, _req.Index);
                searchSw.Stop();
                Observability.RecordStage("ComputeSimilarity", searchSw.Elapsed.TotalMilliseconds,
                    ("result_count", results.Count));

                if (results.Count > 0)
                    return (results, false, false);
            }

            if (canSave)
                return (new List<M_SearchResult>(), false, true);
            return (new List<M_SearchResult>(), false, false);
        }

        // ── L1 path: check RAM then fall back to LoadAndCache ──
        var bucket = node.Buckets.TryGetValue(bucketKey, out var hotBucket)
            ? hotBucket
            : Agent.Services.Cache.BucketCacheManager.LoadAndCache(_bitstring);

        if (bucket != null && bucket.DataCount > 0)
        {
            bucket.TouchAccess();
            var searchSw = System.Diagnostics.Stopwatch.StartNew();
            var localResults = await bucket.SearchData(_vector, _minimum_similarity, _k, _req.Index);
            searchSw.Stop();
            Observability.RecordStage("ComputeSimilarity", searchSw.Elapsed.TotalMilliseconds,
                ("result_count", localResults.Count), ("bucket_size", bucket.data.Count));
            return (localResults, false, false);
        }

        if (canSave)
            return (new List<M_SearchResult>(), false, true);

        return (new List<M_SearchResult>(), false, false);
    }

    public static async Task<M_Bucket.InsertResult> StoreInBucket(M_Node node, string bucket_string, M_Data _data, string HeadRouteID)
    {
        // ── L1 bypass: dedup + store directly via RocksDB ──
        if (!Agent.Services.Cache.BucketCacheManager.L1Enabled)
        {
            var bucketStorage = Agent.Services.Cache.BucketCacheManager.GetBucketStorage();
            ulong bucketKey = RocksDbBucketStorage.BitstringToUlong(bucket_string);

            if (_data.chunk != null && _data.chunk.Length > 0 && _data.vector != null && _data.vector.Length > 0 && bucketStorage != null)
            {
                float queryNormSq = _data.normSquared > 0f
                    ? _data.normSquared
                    : Agent.Utils.Misc.Misc.ComputeNormSquared(_data.vector);
                float threshold = Globals.StoreSimilarityThreshold;

                var match = bucketStorage.SearchSingleBucketForStore(
                    bucketKey, _data.vector, queryNormSq, threshold);

                if (match.HasValue)
                {
                    var m = match.Value;
                    _data.storageGuid = m.storageGuid;
                    _data.id = m.bucketId;
                    _data.index = m.bucketIndex;
                    return new M_Bucket.InsertResult
                    {
                        BucketId = m.bucketId,
                        BucketIndex = m.bucketIndex,
                        WasDeduplicated = true,
                        Similarity = m.similarity,
                        MatchedStorageGuid = m.storageGuid
                    };
                }
            }

            var (storedBucketId, storedBucketIndex) = await NetworkFileStorageHandler.StoreVector(bucket_string, _data);
            _data.id = storedBucketId;
            _data.index = storedBucketIndex;
            return new M_Bucket.InsertResult { BucketId = storedBucketId, BucketIndex = storedBucketIndex };
        }

        // ── L1 path: load bucket into RAM, dedup via M_Bucket.InsertData ──
        var bucket = Agent.Services.Cache.BucketCacheManager.GetOrLoad(bucket_string);
        var result = await bucket.InsertData(_data, 0);

        if (!result.WasDeduplicated)
            Agent.Services.Cache.BucketCacheManager.NotifyBucketGrew(M_Bucket.EstBytesPerVectorPublic);
        return result;
    }

}
