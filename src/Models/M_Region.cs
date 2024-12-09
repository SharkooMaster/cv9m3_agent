
namespace Agent.Models;

public class M_Region
{
    public float x_min { get; set; }
    public float x_max { get; set; }

    public float y_min { get; set; }
    public float y_max { get; set; }

    public bool in_range(float _x, float _y)
    {
        return (x_min <= _x && _x <= x_max) && (y_min <= _y && _y <= y_max);
    }
}
