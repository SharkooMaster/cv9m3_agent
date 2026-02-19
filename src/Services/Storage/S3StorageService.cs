using System.Security.Cryptography;
using System.Text;
using Agent.Interfaces.Infs;
using Amazon.S3;
using Amazon.S3.Model;
using Npgsql;
using System.Collections.Concurrent;

namespace Agent.Services.Storage;

public class S3StorageService : INetworkFileStorageService
{
    private readonly string _bucketName;
    private readonly string _postgresConnectionString;
    private readonly NpgsqlDataSource _dataSource;
    private readonly IAmazonS3 _s3Client;
    private static readonly SemaphoreSlim _dbSemaphore = new(GetDbConcurrencyLimit(), GetDbConcurrencyLimit());
    private static readonly ConcurrentDictionary<string, long> _missingBucketCache = new();

    public S3StorageService(
        string bucketName,
        string endpoint,
        string accessKey,
        string secretKey,
        bool forcePathStyle,
        bool useSsl,
        string postgresConnectionString)
    {
        _bucketName = bucketName;
        _postgresConnectionString = BuildConnectionString(postgresConnectionString);
        _dataSource = NpgsqlDataSource.Create(_postgresConnectionString);

        var config = new AmazonS3Config
        {
            ServiceURL = endpoint,
            ForcePathStyle = forcePathStyle,
            UseHttp = !useSsl
        };
        _s3Client = new AmazonS3Client(accessKey, secretKey, config);
    }

    private static int GetDbConcurrencyLimit()
    {
        var raw = Environment.GetEnvironmentVariable("AGENT_DB_CONCURRENCY");
        if (int.TryParse(raw, out var v) && v > 0)
            return v;
        return 24;
    }

    private static int GetMissingBucketTtlSeconds()
    {
        var raw = Environment.GetEnvironmentVariable("AGENT_MISSING_BUCKET_TTL_SEC");
        if (int.TryParse(raw, out var v) && v > 0)
            return v;
        return 20;
    }

    private static string BuildConnectionString(string raw)
    {
        var b = new NpgsqlConnectionStringBuilder(raw);
        if (b.MaxPoolSize <= 0)
            b.MaxPoolSize = 120;
        if (b.MinPoolSize < 0)
            b.MinPoolSize = 0;
        return b.ConnectionString;
    }

    private static bool IsKnownMissingBucket(string bucketId)
    {
        if (!_missingBucketCache.TryGetValue(bucketId, out var ticks))
            return false;
        var age = DateTime.UtcNow - new DateTime(ticks, DateTimeKind.Utc);
        return age.TotalSeconds <= GetMissingBucketTtlSeconds();
    }

    private static void MarkMissingBucket(string bucketId)
        => _missingBucketCache[bucketId] = DateTime.UtcNow.Ticks;

    private static void ClearMissingBucket(string bucketId)
        => _missingBucketCache.TryRemove(bucketId, out _);

    public async Task<M_Bucket> ReadBucket(string bucket_Id)
    {
        var result = new M_Bucket(bucket_Id);
        if (IsKnownMissingBucket(bucket_Id))
        {
            return result;
        }

        var rows = await GetVectorsByBucketAsync(bucket_Id);
        if (rows.Count == 0)
        {
            MarkMissingBucket(bucket_Id);
            return result;
        }
        ClearMissingBucket(bucket_Id);

        // Lazy loading to avoid loading all chunk bytes from S3 on read.
        foreach (var (vector, storageGuid, id, index) in rows)
        {
            result.data.Add(new M_Data
            {
                vector = vector,
                chunk = null,
                storageGuid = storageGuid,
                id = (ulong)id,
                index = (ulong)index
            });
        }

        return result;
    }

    public async Task<(int, int)> StoreVector(string bucket_Id, M_Data data)
    {
        if (data.chunk == null || data.chunk.Length == 0)
            throw new ArgumentException("Chunk data cannot be null or empty", nameof(data));
        if (data.vector == null || data.vector.Length == 0)
            throw new ArgumentException("Vector data cannot be null or empty", nameof(data));

        string objectName = $"chunks/{GenerateChunkKey(data.chunk)}";
        await PutChunkAsync(objectName, data.chunk);
        return await InsertChunkMetadataAsync(data.vector, objectName, data.chunk.Length, bucket_Id);
    }

    public async Task<byte[]?> GetChunkAsync(string storageGuid)
    {
        if (string.IsNullOrWhiteSpace(storageGuid))
            return null;

        try
        {
            using var response = await _s3Client.GetObjectAsync(new GetObjectRequest
            {
                BucketName = _bucketName,
                Key = storageGuid
            });
            await using var ms = new MemoryStream();
            await response.ResponseStream.CopyToAsync(ms);
            return ms.ToArray();
        }
        catch (AmazonS3Exception ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound || ex.ErrorCode == "NoSuchKey")
        {
            return null;
        }
    }

    public async Task<byte[]?> GetChunkByReferenceAsync(ulong bucketId, ulong bucketIndex)
    {
        await _dbSemaphore.WaitAsync();
        try
        {
            await using var conn = await _dataSource.OpenConnectionAsync();

            var cmd = new NpgsqlCommand(@"
            SELECT storage_guid
            FROM vectors
            WHERE bucket_id = @bucketId AND bucket_index = @bucketIndex
            LIMIT 1;
        ", conn);
            cmd.Parameters.AddWithValue("@bucketId", (long)bucketId);
            cmd.Parameters.AddWithValue("@bucketIndex", (long)bucketIndex);

            var storageGuidObj = await cmd.ExecuteScalarAsync();
            var storageGuid = Convert.ToString(storageGuidObj);
            if (string.IsNullOrWhiteSpace(storageGuid))
                return null;

            return await GetChunkAsync(storageGuid);
        }
        finally
        {
            _dbSemaphore.Release();
        }
    }

    private async Task PutChunkAsync(string objectName, byte[] bytes)
    {
        await using var ms = new MemoryStream(bytes);
        await _s3Client.PutObjectAsync(new PutObjectRequest
        {
            BucketName = _bucketName,
            Key = objectName,
            InputStream = ms
        });
    }

    private static string GenerateChunkKey(byte[] chunkData)
    {
        using var sha256 = SHA256.Create();
        var hashBytes = sha256.ComputeHash(chunkData);
        var sb = new StringBuilder(hashBytes.Length * 2);
        foreach (var b in hashBytes)
        {
            sb.Append(b.ToString("x2"));
        }
        return sb.ToString();
    }

    private async Task<(int, int)> InsertChunkMetadataAsync(float[] vector, string storagePath, int size, string bucketName)
    {
        await _dbSemaphore.WaitAsync();
        try
        {
            await using var conn = await _dataSource.OpenConnectionAsync();

            int bucketId;
            var checkBucketCmd = new NpgsqlCommand("SELECT id FROM bucket_keys WHERE bucket_name = @bucketName LIMIT 1", conn);
            checkBucketCmd.Parameters.AddWithValue("@bucketName", bucketName);
            var existingId = await checkBucketCmd.ExecuteScalarAsync();

            if (existingId != null)
            {
                bucketId = (int)existingId;
            }
            else
            {
                var insertBucketCmd = new NpgsqlCommand(@"
                INSERT INTO bucket_keys (bucket_name, usage_count, next_index)
                VALUES (@bucketName, 0, 1)
                RETURNING id
            ", conn);
                insertBucketCmd.Parameters.AddWithValue("@bucketName", bucketName);
                bucketId = (int)(await insertBucketCmd.ExecuteScalarAsync())!;
            }

            var getNextIndexCmd = new NpgsqlCommand(@"
            UPDATE bucket_keys
            SET next_index = next_index + 1
            WHERE id = @bucketId
            RETURNING next_index - 1 AS bucket_index
        ", conn);
            getNextIndexCmd.Parameters.AddWithValue("@bucketId", bucketId);
            int bucketIndex = (int)(await getNextIndexCmd.ExecuteScalarAsync())!;

            var insertVectorCmd = new NpgsqlCommand(@"
            INSERT INTO vectors (vector, storage_guid, size, created_at, bucket_id, bucket_index)
            VALUES (@vector, @storagePath, @size, NOW(), @bucketId, @bucketIndex)
        ", conn);
            insertVectorCmd.Parameters.AddWithValue("@vector", vector);
            insertVectorCmd.Parameters.AddWithValue("@storagePath", storagePath);
            insertVectorCmd.Parameters.AddWithValue("@size", size);
            insertVectorCmd.Parameters.AddWithValue("@bucketId", bucketId);
            insertVectorCmd.Parameters.AddWithValue("@bucketIndex", bucketIndex);
            await insertVectorCmd.ExecuteNonQueryAsync();

            return (bucketId, bucketIndex);
        }
        finally
        {
            _dbSemaphore.Release();
        }
    }

    private async Task<List<(float[] vector, string storageGuid, long id, long index)>> GetVectorsByBucketAsync(string bucketName)
    {
        var results = new List<(float[] vector, string storageGuid, long id, long index)>();
        await _dbSemaphore.WaitAsync();
        try
        {
            await using var conn = await _dataSource.OpenConnectionAsync();

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
                results.Add((
                    reader.GetFieldValue<float[]>(0),
                    reader.GetString(1),
                    reader.GetInt32(2),
                    reader.GetInt32(3)));
            }

            return results;
        }
        finally
        {
            _dbSemaphore.Release();
        }
    }

    public async Task<List<(float[] vector, string storageGuid, long bucketId, long bucketIndex)>> GetVectorsByBucketsAsync(List<string> bucketNames)
    {
        // S3 backend not actively used — stub for interface compliance
        throw new NotSupportedException("GetVectorsByBucketsAsync not supported on S3StorageService. Use RocksDB backend.");
    }
}

