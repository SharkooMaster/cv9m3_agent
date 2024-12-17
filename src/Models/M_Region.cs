
using System.Numerics;

namespace Agent.Models;

public class M_Region
{
    public Vector2 _min { get; set; }
    public Vector2 _max { get; set; }

    public bool in_range(float _x, float _y)
    {
        return (_min.X <= _x && _x <= _max.X) && (_min.Y <= _y && _y <= _max.Y);
    }
}
