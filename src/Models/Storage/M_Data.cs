
using Agent.Utils.Globals;

public class M_Data
{
    public int id { get; set; }
    public float[] vector { get; set; }
    public string metadata { get; set; }
    public List<int> rpu = new List<int>(); // Requests per unit

    public void IncrementRPU() {
        rpu[Globals.RPU_SECTION] += 1;
    }

    public void ClearRPU()
    {
        rpu.Clear();
        rpu.AddRange(new int[] {0,0,0});
    }
}
