
using System.Net.Http.Headers;
using System.Numerics;
using Agent.Modules.Agneta;
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
    public int node_pos {get;set;}
    public M_Node? successor {get;set;}
    public M_Node? predeccessor {get;set;}

    public int m = 64; // Ring Size

    // ROUTING
    public Dictionary<int, M_Node> finger_table = new Dictionary<int, M_Node>();

    // DATA
    public Dictionary<string, M_VectorBucket> buckets = new Dictionary<string, M_VectorBucket>();
}
