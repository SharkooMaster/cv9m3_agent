using System.Collections.Concurrent;
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

/*     public static async Task<M_Node> JoinNetwork(M_Node node, string bootstrap_node)
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

        await NotifyFingersOfNewNode(node);

        return node;
    } */

    public static async Task<M_Node> JoinNetwork(M_Node node, string bootstrap_node)
    {
        try
        {
            if(bootstrap_node == null)
            {
                await AgnetaHandler.Log(0, "Only node in the network");
                node.successor = new M_Node() { id = node.id, ip = node.ip };
                node.predecessor = new M_Node() { id = node.id, ip = node.ip };
                return await InitFingerTable(node);
            }

            // Get successor with retry
            string successor_ip = null;
            for (int attempt = 0; attempt < 3 && successor_ip == null; attempt++)
            {
                try
                {
                    successor_ip = await S_FindPeerResponsible(node.id, bootstrap_node);
                    if (string.IsNullOrEmpty(successor_ip)) throw new Exception("Empty successor IP");
                }
                catch (Exception ex)
                {
                    await AgnetaHandler.Log(1, $"Join attempt {attempt + 1} failed: {ex.Message}");
                    await Task.Delay(100 * (attempt + 1));
                }
            }

            if (successor_ip == null)
            {
                throw new Exception("Failed to join network - could not find successor");
            }

            // Get successor info
            var getSuccessor_res = await _getNodeInfoService.ClientGet(successor_ip);
            node.successor = new M_Node() { id = getSuccessor_res.Id, ip = getSuccessor_res.Ip };

            // Rest of your join logic with similar error handling
            // ...

            return node;
        }
        catch (Exception ex)
        {
            await AgnetaHandler.Log(1, $"JoinNetwork failed: {ex.Message}");
            throw;  // Let the caller handle complete join failure
        }
    }

    public static async Task NotifyFingersOfNewNode(M_Node node)
    {
        // For each finger table entry index i
        for (int i = 0; i < Globals.FINGER_TABLE_SIZE; i++)
        {
            try
            {
                // Calculate who might need to update their ith finger
                // A node p needs to update its ith finger if:
                // p + 2^i falls between predecessor and this node
                ulong backwardDistance = (1UL << i);
                ulong potentialPredecessorId = (node.id >= backwardDistance) ? 
                    (node.id - backwardDistance) : 
                    ((1UL << Globals.FINGER_TABLE_SIZE) - (backwardDistance - node.id));

                // Find the node responsible for this ID
                string predecessorIp = await FindSuccessor(node, potentialPredecessorId);

                if (predecessorIp != node.ip && predecessorIp != node.predecessor?.ip)
                {
                    // Notify this node to update its ith finger
                    await _updateFingerTableService.ClientUpdate(new UpdateFingerTable_Req 
                    { 
                        FingerIndex = i,
                        Id = node.id,
                        Ip = node.ip
                    }, predecessorIp);
                }
            }
            catch (Exception ex)
            {
                await AgnetaHandler.Log(1, $"NotifyFingersOfNewNode: Failed to notify for finger {i}: {ex.Message}");
            }
        }
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

/*     private static async Task<M_Node> BuildFingerTable(M_Node node)
    {
        for (int i = 0; i < Globals.FINGER_TABLE_SIZE; i++)
        {
            ulong _finger = (node.id + (1UL << i)) % (1UL << Globals.FINGER_TABLE_SIZE);
            string _ip = await FindSuccessor(node, _finger);

            GetNodeInfo_Result peer = await _getNodeInfoService.ClientGet(_ip);
            node.fingerTable.TryAdd(_finger, new M_Node() { id = peer.Id, ip = peer.Ip });
        }
        return node;
    } */

    private static async Task<M_Node> BuildFingerTable(M_Node node)
    {
        try 
        {
            for (int i = 0; i < Globals.FINGER_TABLE_SIZE; i++)
            {
                try {
                    ulong _finger = (node.id + (1UL << i)) % (1UL << Globals.FINGER_TABLE_SIZE);
                    string _ip = await FindSuccessor(node, _finger);
                    if (string.IsNullOrEmpty(_ip)) continue;

                    GetNodeInfo_Result peer = await _getNodeInfoService.ClientGet(_ip);
                    if (!node.fingerTable.TryAdd(_finger, new M_Node() { id = peer.Id, ip = peer.Ip }))
                    {
                        await AgnetaHandler.Log(1, $"BuildFingerTable: Failed to add finger {i}");
                    }
                }
                catch (Exception ex) {
                    await AgnetaHandler.Log(1, $"BuildFingerTable: Error on finger {i}: {ex.Message}");
                }
            }
            return node;
        }
        catch (Exception ex)
        {
            await AgnetaHandler.Log(1, $"BuildFingerTable: Critical error: {ex.Message}");
            return node;
        }
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
            if(NodeUtils.inBetween(node.fingerTable[fingerTableKeys[i]].id, node.id, target))
            {
                return node.fingerTable[fingerTableKeys[i]];
            }
        }
        return node;
    }

/*     public static async Task<M_Node> VerifySuccessor(M_Node node)
    {
        GetPredecessor_Result getPredecessor_result = await _getPredecessorService.ClientGet(node.successor.ip);
        if(getPredecessor_result.Id != node.id && getPredecessor_result.Ip != node.ip)
        {
            node.successor = new M_Node() { id = getPredecessor_result.Id, ip = getPredecessor_result.Ip };

            UpdatePredecessor_Req updatePredecessor_req = new UpdatePredecessor_Req() { Id = node.id, Ip = node.ip };
            await _updatePredecessorService.ClientUpdate(updatePredecessor_req, node.successor.ip);
        }
        return node;
    } */

    public static async Task<M_Node> VerifySuccessor(M_Node node)
    {
        try
        {
            if (node?.successor == null) return node;
    
            GetPredecessor_Result getPredecessor_result;
            try 
            {
                getPredecessor_result = await _getPredecessorService.ClientGet(node.successor.ip);
                if (getPredecessor_result == null) return node;
            }
            catch (Exception ex)
            {
                await AgnetaHandler.Log(1, $"VerifySuccessor: Failed to get predecessor: {ex.Message}");
                return node;
            }
    
            if(getPredecessor_result.Id != node.id && getPredecessor_result.Ip != node.ip)
            {
                node.successor = new M_Node() { id = getPredecessor_result.Id, ip = getPredecessor_result.Ip };
                
                try 
                {
                    var req = new UpdatePredecessor_Req() { Id = node.id, Ip = node.ip };
                    await _updatePredecessorService.ClientUpdate(req, node.successor.ip);
                }
                catch (Exception ex)
                {
                    await AgnetaHandler.Log(1, $"VerifySuccessor: Failed to update predecessor: {ex.Message}");
                }
            }
            return node;
        }
        catch (Exception ex)
        {
            await AgnetaHandler.Log(1, $"VerifySuccessor: Critical error: {ex.Message}");
            return node;
        }
    }

/*     public static async Task<M_Node> FixFingerTable(M_Node node)
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
    } */

    public static async Task<M_Node> FixFingerTable(M_Node node)
    {
        if(node.fingerTable.Count != Globals.FINGER_TABLE_SIZE) { return node; }

        // Update a single finger each time
        _nextFinger = (_nextFinger + 1 >= Globals.FINGER_TABLE_SIZE) ? 0 : _nextFinger + 1;

        // Calculate the actual finger key/offset
        ulong fingerKey = (node.id + (1UL << _nextFinger)) % (1UL << Globals.FINGER_TABLE_SIZE);

        string _new_successor = await FindSuccessor(node, fingerKey);
        if(_new_successor == node.ip) return node;

        M_Node newFingerEntry;
        if(_new_successor == node.successor.ip)
        {
            newFingerEntry = new M_Node() { id = node.successor.id, ip = node.successor.ip };
        }
        else
        {
            GetNodeInfo_Result getNodeInfo_result = await _getNodeInfoService.ClientGet(_new_successor);
            newFingerEntry = new M_Node() { id = getNodeInfo_result.Id, ip = getNodeInfo_result.Ip };
        }

        // Update the ACTUAL finger entry
        if(!node.fingerTable.TryUpdate(fingerKey, newFingerEntry, node.fingerTable[fingerKey]))
        {
            await AgnetaHandler.Log(1, $"Failed to update finger {_nextFinger} (key: {fingerKey})");
        }

        return node;
    }

}
