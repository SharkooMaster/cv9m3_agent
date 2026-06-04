
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

    /// <summary>
    /// Backpressure signal for the non-blocking store path. Returns true only when the
    /// underlying RocksDB engine has actually stalled/throttled writes. Backends that
    /// don't expose engine pressure (non-RocksDB) report false (never throttle).
    /// </summary>
    public static bool IsUnderWritePressure()
    {
        return _instance is RocksDbStorageService rocksDbService
            && rocksDbService.IsUnderWritePressure();
    }

    public static string GenerateChunkKey(byte[] chunkData)
    {
        // Static HashData avoids allocating (and disposing) a SHA256 instance on every
        // call. Lowercase hex is preserved so keys match data already on disk.
        Span<byte> hash = stackalloc byte[32];
        System.Security.Cryptography.SHA256.HashData(chunkData, hash);
        return Convert.ToHexString(hash).ToLowerInvariant();
    }
}
