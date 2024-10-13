
namespace Agent.Models.Storage;

public enum BucketLocations
{
    MEM,
    SSD,
    NFS
}

public class Bucket
{
    public required string bucketKey { get; set; }

    public int lastSyncTime = 0;
    public int usageCount = 0;
    public DateTime lastUsed = DateTime.Now;
    public BucketLocations location = BucketLocations.MEM;

    public List<BucketRow> bucketRows = new List<BucketRow>();
}
