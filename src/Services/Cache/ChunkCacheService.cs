using Agent.Interfaces.Infs;
using Agent.Services.Storage;

namespace Agent.Services.Cache;

/// <summary>
/// Service for caching chunks with automatic eviction to prevent memory exhaustion.
/// Uses lazy loading - chunks are loaded from disk only when needed.
/// </summary>
public class ChunkCacheService
{
    private readonly MruBlobCache _chunkCache;
    private readonly INetworkFileStorageService _storageService;
    private readonly ILogger<ChunkCacheService>? _logger;

    public ChunkCacheService(
        INetworkFileStorageService storageService,
        long maxCacheSizeBytes = 512 * 1024 * 1024, // 512MB default
        TimeSpan? cacheTtl = null,
        ILogger<ChunkCacheService>? logger = null)
    {
        _storageService = storageService;
        _logger = logger;
        _chunkCache = new MruBlobCache(
            maxSizeBytes: maxCacheSizeBytes,
            ttl: cacheTtl ?? TimeSpan.FromMinutes(30)
        );
    }

    /// <summary>
    /// Gets a chunk from cache or loads it from disk if not cached.
    /// </summary>
    public async Task<byte[]?> GetChunkAsync(string storageGuid)
    {
        // Try cache first
        var cached = _chunkCache.Get(storageGuid);
        if (cached != null)
        {
            _logger?.LogDebug($"Chunk {storageGuid} found in cache ({cached.Length} bytes)");
            return cached;
        }

        // Load from disk using the interface method
        byte[]? chunk = await _storageService.GetChunkAsync(storageGuid);

        if (chunk != null)
        {
            // Cache it for future use
            _chunkCache.Put(storageGuid, chunk);
            _logger?.LogDebug($"Chunk {storageGuid} loaded from disk and cached ({chunk.Length} bytes)");
        }
        else
        {
            _logger?.LogWarning($"Chunk {storageGuid} not found in cache or disk");
        }

        return chunk;
    }

    /// <summary>
    /// Cache-only lookup — returns the chunk if it's in the MRU cache, null otherwise.
    /// Does NOT fall back to disk. Used by GetChunkAsync to avoid missing chunks
    /// that were CacheChunk'd but not yet flushed by the RocksDB write batcher.
    /// </summary>
    public byte[]? GetFromCacheOnly(string storageGuid)
    {
        return _chunkCache.Get(storageGuid);
    }

    /// <summary>
    /// Puts a chunk into the cache (e.g., after storing it).
    /// </summary>
    public void CacheChunk(string storageGuid, byte[] chunkData)
    {
        if (chunkData != null && chunkData.Length > 0)
        {
            _chunkCache.Put(storageGuid, chunkData);
            _logger?.LogDebug($"Chunk {storageGuid} added to cache ({chunkData.Length} bytes)");
        }
    }

    /// <summary>
    /// Removes a chunk from cache.
    /// </summary>
    public bool RemoveChunk(string storageGuid)
    {
        return _chunkCache.Remove(storageGuid);
    }

    /// <summary>
    /// Gets cache statistics.
    /// </summary>
    public (int Count, long SizeBytes) GetCacheStats()
    {
        return (_chunkCache.Count, _chunkCache.CurrentSizeBytes);
    }

    /// <summary>
    /// Clears the entire cache.
    /// </summary>
    public void Clear()
    {
        _chunkCache.Clear();
        _logger?.LogInformation("Chunk cache cleared");
    }

    public void Dispose()
    {
        _chunkCache?.Dispose();
    }
}


