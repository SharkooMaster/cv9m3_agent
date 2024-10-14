
using Google.Protobuf;
using TpInternalService;

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

    public async Task<float> GetSimilarity(float[] vectorA, float[] vectorB)
    {
        if (vectorA.Length != vectorB.Length)
        {
            throw new ArgumentException("Vectors must be of the same length.");
        }

        float dotProduct = 0.0f;
        float magnitudeA = 0.0f;
        float magnitudeB = 0.0f;

        for (int i = 0; i < vectorA.Length; i++)
        {
            dotProduct += vectorA[i] * vectorB[i];
            magnitudeA += MathF.Pow(vectorA[i], 2);
            magnitudeB += MathF.Pow(vectorB[i], 2);
        }

        magnitudeA = MathF.Sqrt(magnitudeA);
        magnitudeB = MathF.Sqrt(magnitudeB);

        return (magnitudeA == 0 || magnitudeB == 0) ? 0 : dotProduct / (magnitudeA * magnitudeB);
    }

    public async Task<List<TpInternalService.Result>> Search(float[] _vector, int _topK, float minThresh)
    {
        List<TpInternalService.Result> results = new List<TpInternalService.Result>();

        foreach (BucketRow row in bucketRows)
        {
            float _score = await GetSimilarity(row.vector, _vector);
            if(_score >= minThresh)
            {
                TpInternalService.Result result = new TpInternalService.Result();
                result.Chunk = ByteString.CopyFrom(row.chunk);
                result.Id = row.id;
                result.Score = _score;

                results.Add(result);
            }
        }
        return results;
    }
}
