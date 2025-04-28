using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Text.Json;
using Agent.Interfaces;
using Agent.Models;
using Agent.Modules.Agneta;
using Agent.Utils;
using Agent.Utils.Globals;
using Agent.Utils.Misc;
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
            await AgnetaHandler.Log(1, "Only node in the network");
            Console.WriteLine("Only node in the network");
            node.successor   = new M_Node() { id = node.id, ip = node.ip };
            node.predecessor = new M_Node() { id = node.id, ip = node.ip };

            return node;
        }

        // Get my successor
        string successor_ip = await S_FindPeerResponsible(node.id, bootstrap_node);
        GetNodeInfo_Result getSuccessor_res = await _getNodeInfoService.ClientGet(successor_ip);
        M_Node successor = new M_Node() { id = getSuccessor_res.Id, ip = getSuccessor_res.Ip };

        node.successor = successor;

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
            M_Node peer = ClosestPreceedingNode(node, target);
            return await S_FindPeerResponsible(target, peer.ip);
        }
    }
    
    private static M_Node ClosestPreceedingNode(M_Node node, ulong target)
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

            GetPredecessor_Result getPredecessor_result = await _getPredecessorService.ClientGet(node.successor.ip, CancellationToken.None);

            if (getPredecessor_result == null)
            {
                Console.WriteLine($"[VerifySuccessor] Warning: No predecessor info received from {node.successor.ip}.");
                return node;
            }

            if (getPredecessor_result.Id != node.id && getPredecessor_result.Ip != node.ip)
            {
                Console.WriteLine($"[VerifySuccessor] Updating successor to {getPredecessor_result.Ip}.");

                node.successor = new M_Node()
                {
                    id = getPredecessor_result.Id,
                    ip = getPredecessor_result.Ip
                };

                try
                {
                    UpdatePredecessor_Req updatePredecessor_req = new UpdatePredecessor_Req()
                    {
                        Id = node.id,
                        Ip = node.ip
                    };
                    await _updatePredecessorService.ClientUpdate(updatePredecessor_req, node.successor.ip);
                }
                catch (Exception updateEx)
                {
                    Console.WriteLine($"[VerifySuccessor] Warning: Failed to update new successor's predecessor: {updateEx.Message}");
                }
            }
        }
        catch (Grpc.Core.RpcException rpcEx)
        {
            Console.WriteLine($"[VerifySuccessor] RpcException while verifying successor {node.successor.ip}: {rpcEx.Status}");
            // Optionally mark successor as dead here, or set node.successor = node (self-loop) temporarily.
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[VerifySuccessor] Exception while verifying successor: {ex.Message}");
        }

        return node;
    }

    public static async Task<(List<M_SearchResult>, bool)> SearchAll(M_Node node, string _bitstring, float[] _vector, float _minimum_similarity, int _k, SearchVector_Req _req, ServerCallContext context)
    {
        //Console.Writeline("Searching");
        bool is_inRange = Agent.Utils.Misc.Misc.IsKeyInRange(node.id, Globals._NODE.successor.id, _bitstring);

        if (is_inRange)
        {
            if(node.Buckets.ContainsKey(_bitstring))
            {
                return (await node.Buckets[_bitstring].SearchData(_vector, _minimum_similarity, _k), false);
            }
            else
            {
                M_Bucket read_bucket = await NetworkFileStorageHandler.ReadBucket(_bitstring);
                if(read_bucket.data.Count > 0)
                {
                    if(!node.Buckets.TryAdd(_bitstring, read_bucket))
                    {
                        Console.WriteLine("Failed to import bucket from NFS");
                    }
                    else
                    {
                        return (await node.Buckets[_bitstring].SearchData(_vector, _minimum_similarity, _k), false);
                    }
                }
            }
            return (new List<M_SearchResult>(), false);
        }
        else
        {
            Console.WriteLine("Out of range");
            try
            {
                return (new List<M_SearchResult>(), true);
            }
            catch (System.Exception)
            {
                Console.WriteLine("ERROR: Couldnt forward request to correct peer (successor)");
                throw;
            }
        }
    }

    public static async Task<ulong> StoreInBucket(M_Node node, string bucket_string, M_Data _data, string HeadRouteID)
    {
        var bucket = node.Buckets.GetOrAdd(bucket_string, _ => new M_Bucket(bucket_string));
        ulong _id = await bucket.BookId();

        string methodName = $"StoreInBucket::{DateTime.Now:HH:mm:ss.fff}_{Guid.NewGuid()}";
        await BackgrounfServiceManager.RegisterFireMethod(methodName, async () =>
        {
            await bucket.InsertData(_data, _id);
        });

        return _id;
    }

}
