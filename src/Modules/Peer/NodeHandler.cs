using Agent.Interfaces;
using Agent.Models;
using Agent.Utils;
using Agent.Utils.Globals;

namespace Agent.Modules.Peer;

public static class NodeService
{
    public static Task BuildFingerTable()
    {
        /*
            * A position in a finger table for i < m : ((id + 2^i) % 2^m) (m = hashring max).
            * Use the dht to look up each one of those keys.
        */
        throw new NotImplementedException();
    }

    public static async Task JoinNetwork(M_Node _node, string bootstrap_node_ip)
    {
        /*
            * Assign ID to this node    [x]
            * Find successor node through bootstrap_node
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
            M_Node _successor = new M_Node();
            string _successor_ip = await FindPeerResponsible(_node, _node.id);
        }
    }

    public static async Task<string> FindPeerResponsible(M_Node _node, ulong target)
    {
        bool isFound = false;
        int indexOfResult = -1;

        // Search finger table for a valid peer
        ulong[] fingerTableKeys = _node.fingerTable.Keys.ToArray();
        for (int i = _node.fingerTable.Count; i > 0; i--)
        {
            if(NodeUtils.inBetween(target, _node.id, fingerTableKeys[i]))
            {
                isFound = true;
                indexOfResult = i;
            }
        }

        // Ask result found if they are responsible for the key to make sure theres no predecessor thats a better fit, and so on
        FindPeerResponsibleService fprs = new FindPeerResponsibleService();
        QueryReq req = new QueryReq() { Val=target };
        QueryRes result = await fprs.ClientFind(req, _node.fingerTable[fingerTableKeys[indexOfResult]].ip);

        return result.Res;
    }
}
