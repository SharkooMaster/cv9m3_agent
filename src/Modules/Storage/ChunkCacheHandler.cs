using Agent.Services.Cache;
using Agent.Interfaces.Infs;

namespace Agent.Modules.Storage;

public static class ChunkCacheHandler
{
    private static ChunkCacheService? _instance;
    public static ChunkCacheService? instance => _instance;

    public static void SetInstance(ChunkCacheService instance)
    {
        _instance = instance ?? throw new ArgumentNullException(nameof(instance));
    }

    public static async Task<byte[]?> GetChunkAsync(string storageGuid)
    {
        if (string.IsNullOrEmpty(storageGuid))
            return null;

        if (_instance != null)
        {
            return await _instance.GetChunkAsync(storageGuid);
        }
        // Fallback to direct storage service if cache not initialized
        return await NetworkFileStorageHandler.instance.GetChunkAsync(storageGuid);
    }

    public static void CacheChunk(string storageGuid, byte[] chunkData)
    {
        _instance?.CacheChunk(storageGuid, chunkData);
    }
}

