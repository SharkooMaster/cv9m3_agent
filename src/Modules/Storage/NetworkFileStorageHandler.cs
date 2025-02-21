
using Agent.Services.Storage;

public static class NetworkFileStorageHandler
{
    private static NetworkFileStorageService _instance;
    public static NetworkFileStorageService instance => _instance ?? throw new InvalidOperationException("Failed to initialize NFS_Handler");

    public static void SetInstance(NetworkFileStorageService instance)
    {
        _instance = instance ?? throw new ArgumentNullException(nameof(instance));
    }

    public static async Task StoreVector(string bucket_Id, M_Data data)
    {
        await _instance.StoreVector(bucket_Id, data);
    }

    public static async Task<M_Bucket> ReadBucket(string bucket_Id)
    {
        return await _instance.ReadBucket(bucket_Id);
    }
}
