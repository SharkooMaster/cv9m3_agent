using System.Data;
using System.Text.Json;
using Newtonsoft.Json;
namespace Agent.Models.Misc;

public class NeighbourData
{
    [JsonProperty("node_type")]
    public string NodeType { get; set; }

    [JsonProperty("load_score")]
    public double LoadScore { get; set; }
    public int Id { get; set; }
    public string Data { get; set; }
}