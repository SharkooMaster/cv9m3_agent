
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
        // successor = _bootstrap_node.
    }

    public string find_successor(int position)
    {
        if(successor != null && NodeUtils.is_between(position, node_pos, successor.node_pos))
        {
            return successor.node_ip;
        }
    }

}
