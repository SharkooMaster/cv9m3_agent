
using System.Collections.Concurrent;

namespace Agent.Models;

public class M_Node
{
    public ulong id { get; set; }
    public string ip { get; set; }
    public string port = "5000";

    public M_Node predecessor {get; set;}
    public M_Node successor {get; set;}

    public ConcurrentDictionary<ulong, M_Node> fingerTable = new ConcurrentDictionary<ulong, M_Node>();
    public ConcurrentDictionary<ulong, M_Bucket> Buckets = new ConcurrentDictionary<ulong, M_Bucket>();

    public M_Node()
    {
    }
}
