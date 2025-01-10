using System.Linq.Expressions;
using Agent.Interfaces;
using Agent.Models;
using Agent.Modules.Agneta;
using Agent.Utils;
using Agent.Utils.Globals;
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

    public static int _nextFinger = 0;

    public static async Task<M_Node> JoinNetwork(M_Node node, string bootstrap_node)
    {
        if(bootstrap_node == null)
        {
            await AgnetaHandler.Log(0, "Only node in the network");
            await AgnetaHandler.Log(1, "Only node in the network");
            node.successor   = new M_Node() { id = node.id, ip = node.ip };
            node.predecessor = new M_Node() { id = node.id, ip = node.ip };

            node = await InitFingerTable(node);

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

        node = await BuildFingerTable(node);

        return node;
    }

    private static async Task<M_Node> InitFingerTable(M_Node node)
    {
        for (int i = 0; i < Globals.FINGER_TABLE_SIZE; i++)
        {
            ulong _finger = (node.id + (1UL << i)) % (1UL << Globals.FINGER_TABLE_SIZE);
            node.fingerTable.TryAdd(_finger, new M_Node() { id = node.id, ip = node.ip });
        }
        return node;
    }

    private static async Task<M_Node> BuildFingerTable(M_Node node)
    {
        for (int i = 0; i < Globals.FINGER_TABLE_SIZE; i++)
        {
            ulong _finger = (node.id + (1UL << i)) % (1UL << Globals.FINGER_TABLE_SIZE);
            string _ip = await FindSuccessor(node, _finger);

            GetNodeInfo_Result peer = await _getNodeInfoService.ClientGet(_ip);
            node.fingerTable.TryAdd(_finger, new M_Node() { id = peer.Id, ip = peer.Ip });
        }
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
            M_Node peer = await ClosestPreceedingNode(node, target);
            return await S_FindPeerResponsible(target, peer.ip);
        }
    }
    
    private static async Task<M_Node> ClosestPreceedingNode(M_Node node, ulong target)
    {
        ulong[] fingerTableKeys = node.fingerTable.Keys.ToArray();
        for (int i = Globals.FINGER_TABLE_SIZE - 1; i >= 0; i--)
        {
            if(NodeUtils.inBetween(fingerTableKeys[i], node.id, target))
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

    public static async Task<M_Node> FixFingerTable(M_Node node)
    {
        if(node.fingerTable.Count != Globals.FINGER_TABLE_SIZE){ return node; }

        _nextFinger = (_nextFinger + 1 >= Globals.FINGER_TABLE_SIZE) ? 0 : _nextFinger + 1;
        ulong target = (node.id + (1UL << _nextFinger)) % (1UL << Globals.FINGER_TABLE_SIZE);

        ulong[] fingerTableKeys = node.fingerTable.Keys.ToArray();
        string _new_successor = await FindSuccessor(node, target);
        if(_new_successor == node.ip){ return node; }

        M_Node newFingerEntry;
        if(_new_successor == node.ip)
        {
            newFingerEntry = new M_Node() { id = node.id, ip = node.ip };
        }
        else if(_new_successor == node.successor.ip)
        {
            newFingerEntry = new M_Node() { id = node.successor.id, ip = node.successor.ip };
        }
        else
        {
            GetNodeInfo_Result getNodeInfo_result = await _getNodeInfoService.ClientGet(_new_successor);
            newFingerEntry = new M_Node() { id = getNodeInfo_result.Id, ip = getNodeInfo_result.Ip };
        }

        ulong fingerKey = fingerTableKeys[_nextFinger];
        if(!node.fingerTable.TryUpdate(fingerKey, newFingerEntry, node.fingerTable[fingerKey]))
        {
            await AgnetaHandler.Log(1, $"Failed to update fingerTablekey[{fingerKey}] with newFingerEntry: {newFingerEntry.ip} from old: {node.fingerTable[fingerKey].ip}");
        }

        return node;
    }
}
