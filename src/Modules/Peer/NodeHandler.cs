using Agent.Interfaces;

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

    public static Task JoinNetwork(string bootstrap_node_ip)
    {
        /*
            * assign ID to this node
            * Find successor node through bootstrap_node
            * Get predecessor node from successor and update there routings
            * Build finger table
        */
        throw new NotImplementedException();
    }
}
