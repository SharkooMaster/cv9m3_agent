using Agent.Interfaces;
using Agent.Models;
using Agent.Utils;

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

    public static Task JoinNetwork(M_Node _node, string bootstrap_node_ip)
    {
        /*
            * assign ID to this node    [x]
            * Find successor node through bootstrap_node
            * Get predecessor node from successor and update there routings
            * Build finger table
        */
        _node.id = NodeUtils.generateNodeID();
        throw new NotImplementedException();
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

        // ask result found if they are responsible for the key to make sure theres no predecessor thats a better fit, and so on

        return "";
    }
}
