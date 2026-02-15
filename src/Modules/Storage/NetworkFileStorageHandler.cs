
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

    public static async Task<(int,int)> StoreVector(string bucket_Id, M_Data data)
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
}
