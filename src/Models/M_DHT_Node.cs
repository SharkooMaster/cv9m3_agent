
using System.Numerics;
using Agent.Models.Misc;

namespace Models;
public class M_DHT_Node
{
    public string? Ip { get; set; }
    public Dictionary<Vector2, ServiceData> FingerTable = new Dictionary<Vector2, ServiceData>();
}
