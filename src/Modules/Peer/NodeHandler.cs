using Agent.Interfaces;
using Agent.Models;
using Agent.Modules.Agneta;
using Agent.Utils;
using Agent.Utils.Globals;
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

    public static async Task<M_Node> JoinNetwork(M_Node node, string bootstrap_node)
    {
        if(bootstrap_node == null)
        {
            await AgnetaHandler.Log(0, "Only node in the network");
            await AgnetaHandler.Log(1, "Only node in the network");
            node.successor = node;
            node.predecessor = node;
            return node;
        }

        string successor_ip = await S_FindPeerResponsible(node.id, bootstrap_node);
        GetNodeInfo_Result getSuccessor_res = await _getNodeInfoService.ClientGet(successor_ip);
        M_Node successor = new M_Node() { id = getSuccessor_res.Id, ip = getSuccessor_res.Ip };

        node.successor = successor;

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
        if(NodeUtils.inBetween(target, node.predecessor.id, node.id))
        {
            return node.ip;
        }

        ulong[] fingerTableKeys = node.fingerTable.Keys.ToArray();
        ulong resulting_key = node.successor.id;
        for (int i = 0; i < node.fingerTable.Count; i++)
        {
            if(node.fingerTable[fingerTableKeys[i]].id >= target)
            {
                resulting_key = fingerTableKeys[i];
                break;
            }
        }

         return await S_FindPeerResponsible(target, node.fingerTable[resulting_key].ip);
    }
}
