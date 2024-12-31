using Agent.Interfaces;
using Agent.Models;
using Agent.Utils;
using Agent.Utils.Globals;

namespace Agent.Modules.Peer;

public static class NodeService
{
    public static async Task BuildFingerTable(M_Node _node)
    {
        /*
            * A position in a finger table for i < m : ((id + 2^i) % 2^m) (m = hashring max).
            * Use the dht to look up each one of those keys.
        */
        for (int i = 0; i < Globals.FINGER_TABLE_SIZE; i++)
        {
            ulong fingerStart = (_node.id + (1UL << i)) % (1UL << Globals.FINGER_TABLE_SIZE);

            FindPeerResponsibleService fprs = new FindPeerResponsibleService();
            QueryReq req = new QueryReq() { Val = fingerStart };
            QueryRes res = await fprs.ClientFind(req, _node.successor.ip);

            GetNodeInfoService gnis = new GetNodeInfoService();
            GetNodeInfo_Result gnis_res = await gnis.ClientGet(res.Res);

            _node.fingerTable[fingerStart] = new M_Node()
            {
                id = gnis_res.Id,
                ip = gnis_res.Ip
            };
        }
    }

    public static async Task JoinNetwork(M_Node _node, string bootstrap_node_ip)
    {
        /*
            * Assign ID to this node    [x]
            * Find successor node through bootstrap_node [x]
            * Get predecessor node from successor and update there routings
            * Build finger table
        */
        _node.id = NodeUtils.generateNodeID();

        if(bootstrap_node_ip == _node.ip || bootstrap_node_ip == null || bootstrap_node_ip == "")
        {
            // Only peer in the network
            _node.predecessor = _node;
            _node.successor = _node;

            for(int i = 0; i < Globals.FINGER_TABLE_SIZE; i++)
            {
                ulong fingerStart = (_node.id + (1UL << i)) % (1UL << Globals.FINGER_TABLE_SIZE);
                _node.fingerTable[fingerStart] = _node;
            }
        }
        else
        {
            // Use successor to create finger table
            // First get basic info of successor
            M_Node _successor = new M_Node();

            FindPeerResponsibleService fprs = new FindPeerResponsibleService();
            QueryReq req = new QueryReq() { Val=_node.id };
            QueryRes result = await fprs.ClientFind(req, bootstrap_node_ip);
            string _successor_ip = result.Res;
            _successor.ip = _successor_ip;

            GetNodeInfoService gnis = new GetNodeInfoService();
            GetNodeInfo_Result gnis_res = await gnis.ClientGet(_successor_ip);

            _successor.id = gnis_res.Id;

            // Assign new successor
            _successor.predecessor = _node;
            _node.successor = _successor;

            // Get Predecessor from successor
            GetPredecessorService gps = new GetPredecessorService();
            GetPredecessor_Result gps_res = await gps.ClientGet(_successor.ip);

            M_Node _predecessor = new M_Node() { id = gps_res.Id, ip = gps_res.Ip };
            _node.predecessor = _predecessor;

            // Update predecessor
            UpdatePredecessorService ups = new UpdatePredecessorService();
            UpdatePredecessor_Req ups_req = new UpdatePredecessor_Req(){ Id = _node.id, Ip = _node.ip };
            await ups.ClientUpdate(ups_req, _node.successor.ip);

            // Update successor
            UpdateSuccessorService uss = new UpdateSuccessorService();
            UpdateSuccessor_Req uss_req = new UpdateSuccessor_Req(){ Id = _node.id, Ip = _node.ip };
            await uss.ClientUpdate(uss_req, _node.predecessor.ip);

            // Build finger table by communicating with successor
            await BuildFingerTable(_node);
        }
    }

    public static async Task<string> FindPeerResponsible(M_Node _node, ulong target)
    {
        bool isFound = false;
        int indexOfResult = -1;

        // Search finger table for a valid peer
        ulong[] fingerTableKeys = _node.fingerTable.Keys.ToArray();
        for (int i = _node.fingerTable.Count - 1; i >= 0; i--)
        {
            if(NodeUtils.inBetween(target, _node.id, fingerTableKeys[i]))
            {
                isFound = true;
                indexOfResult = i;
            }
        }

        if(isFound)
        {
            // Ask result found if they are responsible for the key to make sure theres no predecessor thats a better fit, and so on
            FindPeerResponsibleService fprs = new FindPeerResponsibleService();
            QueryReq req = new QueryReq() { Val=target };
            QueryRes result = await fprs.ClientFind(req, _node.fingerTable[fingerTableKeys[indexOfResult]].ip);
            return result.Res;
        }
        else
        {
            // either im the peer responsible, or we have an issue
            return _node.ip;
        }
    }
}
