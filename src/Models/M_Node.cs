
using System.Net.Http.Headers;
using System.Numerics;
using Agent.Modules.Agneta;
using Agent.Utils;
using TpInternalService;

namespace Agent.Models;

public class M_Node
{
    /*
        Node ID generation and management
        Successor/predecessor handling
        Finger table implementation
        Join/leave protocols
        Basic routing
    */

    // INFO
    public int node_id {get;set;}
    public string node_ip {get;set;}
    public int node_pos {get;set;}
    public M_Node? successor {get;set;}
    public M_Node? predeccessor {get;set;}

    public int m = 64; // Ring Size

    // ROUTING
    public Dictionary<int, M_Node> finger_table = new Dictionary<int, M_Node>();

    // DATA
    public Dictionary<string, M_VectorBucket> buckets = new Dictionary<string, M_VectorBucket>();

    public M_Node closest_preceding_node(int position)
    {
        int[] finger_table_keys = finger_table.Keys.ToArray();
        for (int i = finger_table.Count; i > 0; i--)
        {
            int finger_pos = finger_table_keys[i];
            M_Node finger_node = finger_table[i];
            
            if(NodeUtils.is_between(finger_pos, node_pos, position))
            {
                return finger_node;
            }
        }
        return this;
    }

    public void initialize_finger_table(M_Node _bootstrap_node = null)
    {
        if(_bootstrap_node == null)
        {
            successor = this;
            predeccessor = this;
            for (int i = 0; i < m; i++)
            {
                finger_table.Add(Convert.ToInt32((node_pos + Math.Pow(2, i)) % Math.Pow(2, m)), this);
            }
        }
    }

    public void join_network(M_Node _bootstrap_node = null)
    {
        successor = _bootstrap_node.find_successor(node_pos);
        M_Node successor_node = _bootstrap_node.successor;
    }

    public M_Node find_successor(int position)
    {
        if(successor != null && NodeUtils.is_between(position, node_pos, successor.node_pos))
        {
            return successor;
        }

        M_Node next_node = closest_preceding_node(position);
        if(next_node == this)
        {
            return successor;
        }
        // send to next_node a grpc request to find_successor with target position
    }

}
