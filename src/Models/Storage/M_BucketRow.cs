
namespace Agent.Models.Storage;

public class BucketRow
{
    public required long id { get; set; }
    public required float[] vector { get; set; }
    public required byte[] chunk { get; set; }
}
