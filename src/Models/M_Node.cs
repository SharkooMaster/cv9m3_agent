
namespace Agent.Models;

public class M_Node
{
    public ulong id { get; set; }
    public string ip { get; set; }
    public string port = "5000";

    public M_Node predecessor {get; set;}
    public M_Node successor {get; set;}

    public Dictionary<ulong, M_Node> fingerTable = new Dictionary<ulong, M_Node>();
    public List<M_Node> successor_list = new List<M_Node>();

    public M_Node()
    {
    }
}
