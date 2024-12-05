
using System.Numerics;

public class M_DHT_Node
{
    public string? Ip { get; set; }
    public Dictionary<Vector2, string> FingerTable = new Dictionary<Vector2, string>();
}