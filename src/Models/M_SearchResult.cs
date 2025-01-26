
using System.Text.Json;

public class M_SearchResult
{
    public int id { get; set; }
    public float similarity { get; set; }
    public JsonElement? metadata { get; set; }
}
