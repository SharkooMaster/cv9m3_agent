
namespace Agent.Models;

public class M_VectorBucket
{
    public string signature { get; set; }
    public Dictionary<string, float[]> vectors = new Dictionary<string, float[]>();
    public Dictionary<string, object> metadata = new Dictionary<string, object>();
}
