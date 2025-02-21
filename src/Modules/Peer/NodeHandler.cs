using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Text.Json;
using Agent.Interfaces;
using Agent.Models;
using Agent.Modules.Agneta;
using Agent.Utils;
using Agent.Utils.Globals;
using Agent.Utils.Misc;
using Grpc.Core;
using Xunit.Sdk;

namespace Agent.Modules.Peer;

public static class NodeService
{
    public static FindPeerResponsibleService _findPeerResponsible = new FindPeerResponsibleService();

    public static GetNodeInfoService _getNodeInfoService = new GetNodeInfoService();
    public static GetPredecessorService _getPredecessorService = new GetPredecessorService();
    public static GetSuccessorService _getSuccessorService = new GetSuccessorService();
    public static GetHealthService _getHealth = new GetHealthService();

    public static UpdateSuccessorService _updateSuccessorService = new UpdateSuccessorService();
    public static UpdatePredecessorService _updatePredecessorService = new UpdatePredecessorService();
    public static UpdateFingerTableService _updateFingerTableService = new UpdateFingerTableService();
    
    public static SearchVectorService _searchVectorService = new SearchVectorService();

    public static int _nextFinger = 0;

    public static async Task<M_Node> JoinNetwork(M_Node node, string bootstrap_node)
    {
        if(bootstrap_node == null)
        {
            await AgnetaHandler.Log(0, "Only node in the network");
            await AgnetaHandler.Log(1, "Only node in the network");
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

        // Read data into memory

        return node;
    }
    
    private static async Task<string> S_FindPeerResponsible(ulong target, string _ip)
    {
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
        GetPredecessor_Result getPredecessor_result = await _getPredecessorService.ClientGet(node.successor.ip);
        if(getPredecessor_result.Id != node.id && getPredecessor_result.Ip != node.ip)
        {
            node.successor = new M_Node() { id = getPredecessor_result.Id, ip = getPredecessor_result.Ip };

            UpdatePredecessor_Req updatePredecessor_req = new UpdatePredecessor_Req() { Id = node.id, Ip = node.ip };
            await _updatePredecessorService.ClientUpdate(updatePredecessor_req, node.successor.ip);
        }
        return node;
    }

    public static async Task<List<M_SearchResult>> SearchAll(M_Node node, string _bitstring, float[] _vector, float _minimum_similarity, int _k, SearchVector_Req _req)
    {
        Console.WriteLine("Searching");
        bool is_inRange = Agent.Utils.Misc.Misc.IsKeyInRange(Globals._NODE.id, Globals._NODE.successor.id, _bitstring);

        if(is_inRange)
        {
            await AgnetaHandler.Log(0, "In range");
            if(node.Buckets.ContainsKey(_bitstring))
            {
                Console.WriteLine("Key exists");
                return await node.Buckets[_bitstring].SearchData(_vector, _minimum_similarity, _k);
            }
            return new List<M_SearchResult>();
        }
        else
        {
            await AgnetaHandler.Log(0, "Not in range");
            List<M_SearchResult> to_return = new List<M_SearchResult>();
            SearchVector_Result res = await _searchVectorService.ClientGet(_req, Globals._NODE.successor.ip);
            foreach (var item in res.Results)
            {
                to_return.Add(new M_SearchResult()
                {
                    id=item.Id,
                    metadata=JsonSerializer.Deserialize<JsonElement>(item.Metadata.ToString()),
                    similarity=item.SimilarityRate
                });
            }
            return to_return;
        }
    }

}
