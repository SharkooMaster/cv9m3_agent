
using Agent.Services.Storage;
using Agent.Interfaces.Infs;

// public static class NetworkFileStorageHandler
// {
//     private static NetworkFileStorageService _instance;
//     public static NetworkFileStorageService instance => _instance ?? throw new InvalidOperationException("Failed to initialize NFS_Handler");
// 
//     public static void SetInstance(NetworkFileStorageService instance)
//     {
//         _instance = instance ?? throw new ArgumentNullException(nameof(instance));
//     }
// 
//     public static async Task StoreVector(string bucket_Id, M_Data data)
//     {
//         await _instance.StoreVector(bucket_Id, data);
//     }
// 
//     public static async Task<M_Bucket> ReadBucket(string bucket_Id)
//     {
//         return await _instance.ReadBucket(bucket_Id);
//     }
// }

public static class NetworkFileStorageHandler
{
    private static INetworkFileStorageService _instance;
    public static INetworkFileStorageService instance => _instance ?? throw new InvalidOperationException("Failed to initialize NFS_Handler");

    public static void SetInstance(INetworkFileStorageService instance)
    {
        _instance = instance ?? throw new ArgumentNullException(nameof(instance));
    }

    public static async Task<(ulong,ulong)> StoreVector(string bucket_Id, M_Data data)
    {
        return await _instance.StoreVector(bucket_Id, data);
    }

    public static async Task<M_Bucket> ReadBucket(string bucket_Id)
    {
        return await _instance.ReadBucket(bucket_Id);
    }

    public static async Task<byte[]?> GetChunkByReferenceAsync(ulong bucketId, ulong bucketIndex)
    {
        return await _instance.GetChunkByReferenceAsync(bucketId, bucketIndex);
    }

    public static async Task<byte[]?> GetChunkAsync(string storageGuid)
    {
        return await _instance.GetChunkAsync(storageGuid);
    }

    public static async Task<List<(float[] vector, string storageGuid, long bucketId, long bucketIndex, string bucketName)>> GetVectorsByBucketsAsync(List<string> bucketNames)
    {
        return await _instance.GetVectorsByBucketsAsync(bucketNames);
    }

    public static async Task StoreChunkByKeyAsync(string chunkKey, byte[] chunkData)
    {
        if (_instance is RocksDbStorageService rocksDbService)
        {
            await rocksDbService.StoreChunkByKeyInternalAsync(chunkKey, chunkData);
        }
        else
        {
            throw new NotSupportedException("StoreChunkByKeyAsync is only supported for RocksDbStorageService");
        }
    }

    /// <summary>
    /// Iterate every (storage_guid, chunk_bytes) tuple in the chunk-store.
    /// Used by the rebalance protocol's StreamVnodeData server side.
    /// Throws <see cref="NotSupportedException"/> on storage backends that
    /// don't support cheap full-scan iteration (e.g. a future S3 backend).
    /// </summary>
    public static IEnumerable<(string storageGuid, byte[] chunkBytes)> EnumerateAllChunks()
    {
        if (_instance is RocksDbStorageService rocksDbService)
        {
            return rocksDbService.EnumerateAllChunks();
        }
        throw new NotSupportedException("EnumerateAllChunks is only supported for RocksDbStorageService");
    }

    /// <summary>
    /// Flush all pending writes to durable storage.
    /// MUST be called before responding to Store/BatchStore RPCs.
    /// After this returns, all chunk bytes + bucket metadata are on SSD.
    /// </summary>
    public static void FlushPendingWrites()
    {
        _instance?.FlushPendingWrites();
    }

    public static string GenerateChunkKey(byte[] chunkData)
    {
        using var sha256 = System.Security.Cryptography.SHA256.Create();
        var hashBytes = sha256.ComputeHash(chunkData);
        var sb = new System.Text.StringBuilder(hashBytes.Length * 2);
        foreach (var b in hashBytes)
            sb.Append(b.ToString("x2"));
        return sb.ToString();
    }
}
