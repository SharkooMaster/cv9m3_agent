
using System;
using System.IO;
using System.Numerics;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using Agent.Interfaces.Infs;
using Agent.Utils.Globals;
using Google.Cloud.Storage.V1;
using Npgsql;

namespace Agent.Services.Storage;

public class VectorInfo
{
    public string Hash { get; set; }
    public string StorageGuid { get; set; }
    public int Size { get; set; }
    public DateTime CreatedAt { get; set; }
}

public class GcsSqlStorageService : INetworkFileStorageService
{
    private StorageClient? _storageClient;
    private readonly string _bucketName;
    private readonly string _postgresConnectionString;
    private readonly object _storageClientLock = new object();

    public GcsSqlStorageService(string bucketName, string postgresConnectionString)
    {
        _bucketName = bucketName;
        _postgresConnectionString = postgresConnectionString;
        // Don't initialize StorageClient here - initialize lazily when needed
    }

    private StorageClient? GetStorageClient()
    {
        if (_storageClient != null)
            return _storageClient;

        lock (_storageClientLock)
        {
            if (_storageClient != null)
                return _storageClient;

            try
            {
                _storageClient = StorageClient.Create();
                return _storageClient;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[Warning] Failed to initialize GCS client: {ex.Message}. GCS operations will be disabled.");
                return null;
            }
        }
    }

    public async Task<M_Bucket> ReadBucket(string bucket_Id)
    {
        try
        {
            M_Bucket toReturn = new M_Bucket(bucket_Id);
            List<(float[] vector, string storageGuid, long _id, long _index)> vectors = await GetVectorsByBucketAsync(bucket_Id);

            foreach ((float[], string, long, long) vec in vectors)
            {
                byte[] _chunk = await GetChunkAsync(vec.Item2);
                if(_chunk == null)
                {
                    Console.WriteLine("Couldnt read chunk while getting bucket");
                }
                toReturn.data.Add(new M_Data(){ vector = vec.Item1, chunk = _chunk, id = (ulong)vec.Item3, index = (ulong)vec.Item4});
            }

            return toReturn;
        }
        catch (System.Exception ex)
        {
            Console.WriteLine($"ERROR:ReadBucket:: {ex.Message} ; {ex.Data}");
            throw;
        }
    }

    public async Task<(int,int)> StoreVector(string bucket_Id, M_Data data)
    {
        (bool a, int b, int c) = await StoreChunkAsync(data.vector, data.chunk, bucket_Id);
        return (b,c);
    }

    public static string GenerateChunkKey(byte[] chunkData)
    {
        using var sha256 = SHA256.Create();
        var hashBytes = sha256.ComputeHash(chunkData);

        // Convert to a readable hex string
        var sb = new StringBuilder();
        foreach (var b in hashBytes)
        {
            sb.Append(b.ToString("x2")); // two-digit hex
        }
        return sb.ToString();
    }


    public async Task<(bool, int, int)> StoreChunkAsync(float[] vector, byte[] chunkData, string bucketID)
    {
        try
        {
             string objectName = $"chunks/{GenerateChunkKey(chunkData)}";

            // Check if chunk already exists in GCS
            /* var existingObjects = _storageClient.ListObjects(_bucketName, objectName);
            foreach (var obj in existingObjects)
            {
                if (obj.Name == objectName)
                {
                    Console.WriteLine($"Chunk {hash} already exists in GCS.");
                    return false; // No need to store again
                }
            }

            // Upload chunk
            using var memoryStream = new MemoryStream(data);
            await _storageClient.UploadObjectAsync(_bucketName, objectName, null, memoryStream); */
            // Console.WriteLine($"Uploaded chunk {hash} to GCS.");

            // Insert metadata into PostgreSQL
            (int bucket_id, int bucket_index) = await InsertChunkMetadataAsync(vector, objectName, chunkData.Length, bucketID);

            return (true, bucket_id, bucket_index);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[Error] StoreChunkAsync failed: {ex.Message}");
            return (false, -1, -1);
        }
    }

    private async Task<(int,int)> InsertChunkMetadataAsync(float[] vector, string storagePath, int size, string bucketName)
    {
        await using var conn = new NpgsqlConnection(_postgresConnectionString);
        await conn.OpenAsync();

        int bucketId;

        // Step 1: Check if bucket exists
        var checkBucketCmd = new NpgsqlCommand(@"
            SELECT id FROM bucket_keys WHERE bucket_name = @bucketName LIMIT 1
        ", conn);

        checkBucketCmd.Parameters.AddWithValue("@bucketName", bucketName);
        var result = await checkBucketCmd.ExecuteScalarAsync();

        if (result != null)
        {
            bucketId = (int)result;
        }
        else
        {
            // Step 2: Insert bucket if it doesn't exist
            var insertBucketCmd = new NpgsqlCommand(@"
              INSERT INTO bucket_keys (bucket_name, usage_count, next_index)
              VALUES (@bucketName, 0, 1)
              RETURNING id
            ", conn);

            insertBucketCmd.Parameters.AddWithValue("@bucketName", bucketName);
            bucketId = (int)(await insertBucketCmd.ExecuteScalarAsync());
        }

        // Step 3: Insert into vectors table
        var getNextIndexCmd = new NpgsqlCommand(@"
              UPDATE bucket_keys
              SET next_index = next_index + 1
              WHERE id = @bucketId
              RETURNING next_index - 1 AS bucket_index
        ", conn);
        getNextIndexCmd.Parameters.AddWithValue("@bucketId", bucketId);

        int bucketIndex = (int)(await getNextIndexCmd.ExecuteScalarAsync());

        // Step 4: Insert into vectors table
        var cmd = new NpgsqlCommand(@"
          INSERT INTO vectors (vector, storage_guid, size, created_at, bucket_id, bucket_index)
          VALUES (@vector, @storagePath, @size, NOW(), @bucketId, @bucketIndex)
        ", conn);

        cmd.Parameters.AddWithValue("@vector", vector);
        cmd.Parameters.AddWithValue("@storagePath", storagePath);
        cmd.Parameters.AddWithValue("@size", size);
        cmd.Parameters.AddWithValue("@bucketId", bucketId);
        cmd.Parameters.AddWithValue("@bucketIndex", bucketIndex);

        await cmd.ExecuteNonQueryAsync();

        return (bucketId, bucketIndex);
    }

    public async Task<List<(float[] vector, string storageGuid, long _id, long _index)>> GetVectorsByBucketAsync(string bucketName)
    {
        var results = new List<(float[], string, long, long)>();

        await using var conn = new NpgsqlConnection(_postgresConnectionString);
        await conn.OpenAsync();

        var cmd = new NpgsqlCommand(@"
            SELECT v.vector, v.storage_guid, v.bucket_id, v.bucket_index
            FROM vectors v
            INNER JOIN bucket_keys b ON v.bucket_id = b.id
            WHERE b.bucket_name = @bucketName;
        ", conn);

        cmd.Parameters.AddWithValue("@bucketName", bucketName);

        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var vector = reader.GetFieldValue<float[]>(0);
            var storageGuid = reader.GetString(1);
            var bucketID = reader.GetInt32(2);
            var bucketIndex = reader.GetInt32(3);

            results.Add((vector, storageGuid, bucketID, bucketIndex));
        }

        Console.WriteLine($"Retrieved {results.Count} vectors for bucket '{bucketName}'.");
        return results;
    }

    public async Task<byte[]?> GetChunkAsync(string storageGuid)
    {
        var client = GetStorageClient();
        if (client == null)
        {
            Console.WriteLine($"[Warning] GCS client not available, cannot download chunk {storageGuid}");
            return null;
        }

        try
        {
            using var memoryStream = new MemoryStream();

            await client.DownloadObjectAsync(
                bucket: _bucketName,
                objectName: storageGuid,
                destination: memoryStream
            );

            Console.WriteLine($"Downloaded chunk {storageGuid} from GCS.");
            return memoryStream.ToArray();
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[Error] Failed to download chunk {storageGuid}: {ex.Message}");
            return null;
        }
    }

    public async Task<byte[]?> GetChunkByReferenceAsync(ulong bucketId, ulong bucketIndex)
    {
        try
        {
            await using var conn = new NpgsqlConnection(_postgresConnectionString);
            await conn.OpenAsync();

            var cmd = new NpgsqlCommand(@"
                SELECT storage_guid
                FROM vectors
                WHERE bucket_id = @bucketId AND bucket_index = @bucketIndex
                LIMIT 1;
            ", conn);
            cmd.Parameters.AddWithValue("@bucketId", (long)bucketId);
            cmd.Parameters.AddWithValue("@bucketIndex", (long)bucketIndex);

            var storageGuidObj = await cmd.ExecuteScalarAsync();
            if (storageGuidObj == null || storageGuidObj == DBNull.Value)
            {
                return null;
            }

            var storageGuid = Convert.ToString(storageGuidObj);
            if (string.IsNullOrWhiteSpace(storageGuid))
            {
                return null;
            }

            return await GetChunkAsync(storageGuid);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[Error] Failed to resolve GCS chunk by reference ({bucketId},{bucketIndex}): {ex.Message}");
            return null;
        }
    }
}
