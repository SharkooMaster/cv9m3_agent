
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
    public int node_id {get;set;}
    public M_Node successor {get;set;}
    public M_Node predeccessor {get;set;}
    public Dictionary<int, M_Node> finger_table = new Dictionary<int, M_Node>();
}