
using System.Numerics;
using Agent.Modules.Agneta;
using Agent.Utils;
using Agent.Utils.Misc;

namespace Agent.Models;

public class M_VectorEntry
{
    /// <summary>
    /// Represents a stored vector with its metadata.
    /// </summary>

    // Properties
    public float[] Vector { get; set; }  // The vector data (e.g., 128-dimensional array)
    public Dictionary<string, object>? Metadata { get; set; }  // Metadata associated with the vector
    public DateTime Timestamp { get; set; }  // Timestamp when the vector was created or stored
    public string BucketId { get; set; }  // Binary signature of the vector's bucket

    // Constructor
    public M_VectorEntry(float[] vector, Dictionary<string, object>? metadata, DateTime timestamp, string bucketId)
    {
        Vector = vector ?? throw new ArgumentNullException(nameof(vector));
        Metadata = metadata ?? new Dictionary<string, object>();
        Timestamp = timestamp;
        BucketId = bucketId ?? throw new ArgumentNullException(nameof(bucketId));
    }

    // ToString override for debugging and logging
    public override string ToString()
    {
        return $"VectorEntry(Vector=({Vector.Length} elements), Metadata={Metadata.Count} items, " +
               $"Timestamp={Timestamp}, BucketId={BucketId})";
    }
}

public class M_SearchResult
{
    float score;
    byte[] chunk;
    string id;
}

public class M_VectorStore
{
    public Dictionary<string, M_VectorEntry> vectors = new Dictionary<string, M_VectorEntry>();
    public Dictionary<string, List<string>> bucket_index = new Dictionary<string, List<string>>();

    public M_Region region = new M_Region();

    public M_VectorStore(M_Region _region)
    {
        region = _region;
    }

    public async Task<string> store_vector(float[] _vector, Dictionary<string, object>? _metadata = null)
    {
        if(vectors == null)
        {
            await AgnetaHandler.Log(2, "Failed to store vector. Vector was null");
            return "Failed";
        }

        string vector_id = Guid.NewGuid().ToString();

        string bucket_id = Agent.Utils.Misc.Misc.vector_to_bitstring(_vector);

        vectors[vector_id] = new M_VectorEntry(
            vector: _vector,
            metadata: _metadata,
            timestamp: DateTime.Now,
            bucketId: bucket_id
        );

        if(!bucket_index.ContainsKey(bucket_id))
        {
            bucket_index.Add(bucket_id, new List<string>());
        }
        bucket_index[bucket_id].Add(vector_id);

        return vector_id;
    }

    public async Task<List<M_SearchResult>> find_similar(float[] query_vector, int k = 10)
    {
        List<M_SearchResult> to_return = new List<M_SearchResult>();

        string query_bucket = Agent.Utils.Misc.Misc.vector_to_bitstring(query_vector);
        Vector2 bucket_coordinates = NodeUtils.calculate_vector_coordinates(query_vector, query_bucket);

        if(!bucket_index.ContainsKey(query_bucket))
        {
            // check if we control that key space
            bool is_in_range = region.in_range(bucket_coordinates.X, bucket_coordinates.Y);
            // if not, forward to correct node
            // if so, return empty
            if(!is_in_range)
            {
            }
            else
            {
                return to_return;
            }
        }

        List<string> candidate_vectors = bucket_index[query_bucket];
        
        string[] neighbor_buckets = NodeUtils.get_neighbor_buckets(query_bucket);
        foreach (string _bucket in neighbor_buckets)
        {
            // check if its in our key range
            if(bucket_index.ContainsKey(_bucket))
            {
            }
            else
            {
            }
        }

        return to_return;
    }
}
