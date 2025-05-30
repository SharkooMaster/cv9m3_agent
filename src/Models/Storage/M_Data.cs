
using System.Text.Json;
using System.Text.Json.Serialization;
using Agent.Utils.Globals;

public class M_Data
{
    public ulong id { get; set; }
    public ulong index { get; set; }
    public float[] vector { get; set; }
    public byte[] chunk { get; set; }
    [JsonIgnore] public List<int> rpu = new List<int>(); // Requests per unit
    
    public void IncrementRPU() => rpu[Globals.RPU_SECTION] += 1;
    public void ClearRPU()
    {
        rpu.Clear();
        rpu.AddRange(new int[] {0,0,0});
    }

    public string ToJson()
    {
        return JsonSerializer.Serialize(this);
    }

    public static M_Data FromJson(string _json)
    {
        return JsonSerializer.Deserialize<M_Data>(_json);
    }
}
