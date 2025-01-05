using Agent.Interfaces;
using Agent.Models;
using Agent.Modules.Agneta;
using Agent.Utils;
using Agent.Utils.Globals;

namespace Agent.Modules.Peer;

public static class NodeService
{
    public static FindPeerResponsibleService _findPeerResponsible = new FindPeerResponsibleService();

    public static GetNodeInfoService _getNodeInfoService = new GetNodeInfoService();
    public static GetPredecessorService _getPredecessorService = new GetPredecessorService();

    public static UpdateSuccessorService _updateSuccessorService = new UpdateSuccessorService();
    public static UpdatePredecessorService _updatePredecessorService = new UpdatePredecessorService();

    public static async Task Join(M_Node _node, string bootstrapNodeIp)
    {
        if (bootstrapNodeIp == null)
        {
            // First node in network
            _node.successor = _node;
            _node.predecessor = _node;
            Globals._NODE = _node;
            return;
        }

        // 1. Find successor through bootstrap node
        QueryReq req = new QueryReq() { Val=_node.id };
        QueryRes res = await _findPeerResponsible.ClientFind(req, bootstrapNodeIp);

        GetNodeInfo_Result successor_info = await _getNodeInfoService.ClientGet(res.Res);
        M_Node _node_successor = new M_Node() { id=successor_info.Id, ip=successor_info.Ip };

        // Get predecessor
        GetPredecessor_Result predecessor_info = await _getPredecessorService.ClientGet(_node_successor.ip);
        M_Node _node_predecessor = new M_Node() { id=predecessor_info.Id, ip=predecessor_info.Ip };

        _node.successor = _node_successor;
        _node.predecessor = _node_predecessor;

        // 2. Update predecessor/successor links
        UpdatePredecessor_Req updatePredecessor_req = new UpdatePredecessor_Req() { Id=_node.id, Ip=_node.ip };
        await _updatePredecessorService.ClientUpdate(updatePredecessor_req, _node.successor.ip);

        UpdateSuccessor_Req updateSuccessor_req = new UpdateSuccessor_Req() { Id=_node.id, Ip=_node.ip };
        await _updateSuccessorService.ClientUpdate(updateSuccessor_req, _node.predecessor.ip);

        // 3. Build finger table
        // 4. Transfer necessary keys
    }

    public static async Task<string> FindSuccessor(M_Node _node, ulong id)
    {
        // If id is between this node and its successor
        // return successor
        if(NodeUtils.inBetween(id, _node.id, _node.successor.id))
        {
            return _node.successor.ip;
        }
        // If id is between predecessor and this node
        // return this node
        if(NodeUtils.inBetween(id, _node.predecessor.id, _node.id))
        {
            return _node.ip;
        }
        // Otherwise, forward to closest preceding finger
        ulong[] fingerTableKeys = _node.fingerTable.Keys.ToArray();
        for (int i = Globals.FINGER_TABLE_SIZE - 1; i <= 0; i--)
        {
            M_Node finger = _node.fingerTable[fingerTableKeys[i]];
            if(NodeUtils.inBetween(finger.id, _node.id, id))
            {
                QueryReq req = new QueryReq() { Val=id };
                QueryRes res = await _findPeerResponsible.ClientFind(req, finger.ip);
                return res.Res;
            }
        }

        QueryReq _req = new QueryReq() { Val=id };
        QueryRes _res = await _findPeerResponsible.ClientFind(_req, _node.successor.ip);
        return _res.Res;
    }

    public static async Task Stabilize(M_Node _node)
    {
        // Periodically verify successor
        // and notify it about this node
    }

    public static async Task FixFingers(M_Node _node)
    {
        // Periodically refresh finger table entries
    }

    public static async Task CheckPredecessor(M_Node _node)
    {
        // Periodically check if predecessor is alive
    }

}
