
using System.Text.Json;

public class M_SearchResult
{
    public ulong id { get; set; }
    public ulong index { get; set; }
    public int i { get; set; }
    public float similarity { get; set; }
    public byte[]? chunk { get; set; }
}
