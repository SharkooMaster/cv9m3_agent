using System;
using System.Collections.Generic;
using System.IO;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using Agent.Interfaces.Infs;
using Agent.Utils.Globals;
using Npgsql;

namespace Agent.Services.Storage;

public class LocalFileStorageService : INetworkFileStorageService
{
    private readonly string _storageDirectory;
    private readonly string _postgresConnectionString;

    public LocalFileStorageService(string storageDirectory, string postgresConnectionString)
    {
        _storageDirectory = storageDirectory;
        _postgresConnectionString = postgresConnectionString;
        
        Console.WriteLine($"[LocalFileStorageService] Initializing with storage directory: {_storageDirectory}");
        
        // Ensure storage directory exists
        Directory.CreateDirectory(_storageDirectory);
        Directory.CreateDirectory(Path.Combine(_storageDirectory, "chunks"));
        
        Console.WriteLine($"[LocalFileStorageService] ✅ Storage directories created/verified at {_storageDirectory}");
    }

    public async Task<M_Bucket> ReadBucket(string bucket_Id)
    {
        try
        {
            M_Bucket toReturn = new M_Bucket(bucket_Id);
            List<(float[] vector, string storageGuid, long _id, long _index)> vectors = await GetVectorsByBucketAsync(bucket_Id);

            // Lazy loading: Store only metadata (vector, storageGuid, IDs) without loading chunks into memory
            // Chunks will be loaded from cache/disk on-demand when needed
            foreach ((float[], string, long, long) vec in vectors)
            {
                toReturn.data.Add(new M_Data() 
                { 
                    vector = vec.Item1, 
                    chunk = null, // Chunk not loaded - will be loaded lazily from cache/disk
                    storageGuid = vec.Item2, // Store path for lazy loading
                    id = (ulong)vec.Item3, 
                    index = (ulong)vec.Item4 
                });
            }

            Console.WriteLine($"ReadBucket: Loaded {vectors.Count} metadata entries for bucket '{bucket_Id}' (chunks will be loaded on-demand)");
            return toReturn;
        }
        catch (System.Exception ex)
        {
            Console.WriteLine($"ERROR:ReadBucket:: {ex.Message} ; {ex.Data}");
            throw;
        }
    }

    public async Task<(int, int)> StoreVector(string bucket_Id, M_Data data)
    {
        if (data.chunk == null || data.chunk.Length == 0)
        {
            Console.WriteLine($"[Warning] StoreVector: chunk data is null or empty for bucket {bucket_Id}");
            throw new ArgumentException("Chunk data cannot be null or empty", nameof(data));
        }
        if (data.vector == null || data.vector.Length == 0)
        {
            Console.WriteLine($"[Warning] StoreVector: vector data is null or empty for bucket {bucket_Id}");
            throw new ArgumentException("Vector data cannot be null or empty", nameof(data));
        }
        (bool a, int b, int c) = await StoreChunkAsync(data.vector, data.chunk, bucket_Id);
        if (!a)
        {
            throw new Exception("Failed to store chunk");
        }
        return (b, c);
    }

    public static string GenerateChunkKey(byte[] chunkData)
    {
        using var sha256 = SHA256.Create();
        var hashBytes = sha256.ComputeHash(chunkData);

        var sb = new StringBuilder();
        foreach (var b in hashBytes)
        {
            sb.Append(b.ToString("x2"));
        }
        return sb.ToString();
    }

    public async Task<(bool, int, int)> StoreChunkAsync(float[] hash, byte[] chunkData, string bucketID)
    {
        try
        {
            string chunkKey = GenerateChunkKey(chunkData);
            string objectName = $"chunks/{chunkKey}";
            string filePath = Path.Combine(_storageDirectory, objectName);

            // Create directory if it doesn't exist
            Directory.CreateDirectory(Path.GetDirectoryName(filePath)!);

            // Check if chunk already exists
            if (File.Exists(filePath))
            {
                Console.WriteLine($"Chunk {chunkKey} already exists locally.");
                // Still insert metadata if needed
            }
            else
            {
                // Write chunk to disk using optimized async I/O
                // Use FileStream with large buffer and async writes for maximum performance
                using (var fileStream = new FileStream(filePath, FileMode.Create, FileAccess.Write, FileShare.None, bufferSize: 131072, useAsync: true))
                {
                    await fileStream.WriteAsync(chunkData, 0, chunkData.Length);
                    // Don't await FlushAsync to avoid blocking - OS will flush when ready
                    fileStream.Flush(true); // Use synchronous flush with no fsync for speed
                }
                
                // Log only occasionally to reduce I/O overhead (every 10th chunk or on errors)
                if (chunkData.Length < 1000 || chunkKey.GetHashCode() % 10 == 0)
                {
                    Console.WriteLine($"[StoreChunkAsync] ✅ Stored chunk {chunkKey} ({chunkData.Length} bytes)");
                }
            }

            // Insert metadata into PostgreSQL
            (int bucket_id, int bucket_index) = await InsertChunkMetadataAsync(hash, objectName, chunkData.Length, bucketID);

            // Cache the chunk after storing (if cache is available)
            try
            {
                Agent.Modules.Storage.ChunkCacheHandler.CacheChunk(objectName, chunkData);
            }
            catch (Exception cacheEx)
            {
                // Cache failure is not critical - chunk is already on disk
                Console.WriteLine($"[StoreChunkAsync] Warning: Failed to cache chunk {chunkKey}: {cacheEx.Message}");
            }

            return (true, bucket_id, bucket_index);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[Error] StoreChunkAsync failed: {ex.Message}");
            return (false, -1, -1);
        }
    }

    private async Task<(int, int)> InsertChunkMetadataAsync(float[] vector, string storagePath, int size, string bucketName)
    {
        await using var conn = new NpgsqlConnection(_postgresConnectionString);
        await conn.OpenAsync();

        int bucketId;

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
            var insertBucketCmd = new NpgsqlCommand(@"
              INSERT INTO bucket_keys (bucket_name, usage_count, next_index)
              VALUES (@bucketName, 0, 1)
              RETURNING id
            ", conn);

            insertBucketCmd.Parameters.AddWithValue("@bucketName", bucketName);
            bucketId = (int)(await insertBucketCmd.ExecuteScalarAsync());
        }

        var getNextIndexCmd = new NpgsqlCommand(@"
              UPDATE bucket_keys
              SET next_index = next_index + 1
              WHERE id = @bucketId
              RETURNING next_index - 1 AS bucket_index
        ", conn);
        getNextIndexCmd.Parameters.AddWithValue("@bucketId", bucketId);

        int bucketIndex = (int)(await getNextIndexCmd.ExecuteScalarAsync());

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
        try
        {
            // storageGuid is already in format "chunks/{hash}" from InsertChunkMetadataAsync
            string filePath = Path.Combine(_storageDirectory, storageGuid);
            
            if (!File.Exists(filePath))
            {
                Console.WriteLine($"[Warning] Chunk file not found: {filePath} (storageGuid: {storageGuid})");
                return null;
            }

            byte[] data = await File.ReadAllBytesAsync(filePath);
            Console.WriteLine($"[GetChunkAsync] Read chunk {storageGuid} from local storage ({data.Length} bytes).");
            return data;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[Error] Failed to read chunk {storageGuid}: {ex.Message}");
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
            Console.WriteLine($"[Error] Failed to resolve chunk by reference ({bucketId},{bucketIndex}): {ex.Message}");
            return null;
        }
    }

    public Task<List<(float[] vector, string storageGuid, long bucketId, long bucketIndex)>> GetVectorsByBucketsAsync(List<string> bucketNames)
        => throw new NotSupportedException("GetVectorsByBucketsAsync not supported on LocalFile backend.");
}

