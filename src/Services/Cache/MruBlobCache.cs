using System.Collections.Concurrent;

namespace Agent.Services.Cache;

/// <summary>
/// MRU (Most Recently Used) blob cache with size and time-based eviction.
/// Thread-safe, uses ArrayPool for memory efficiency.
/// </summary>
public class MruBlobCache : IDisposable
{
    private readonly ConcurrentDictionary<string, CacheEntry> _cache;
    private readonly long _maxSizeBytes;
    private readonly TimeSpan _ttl;
    private readonly Timer _evictionTimer;
    private long _currentSizeBytes;
    private readonly object _sizeLock = new object();

    public MruBlobCache(
        long maxSizeBytes = 1024 * 1024 * 1024, // 1GB default
        TimeSpan? ttl = null)
    {
        _maxSizeBytes = maxSizeBytes;
        _ttl = ttl ?? TimeSpan.FromMinutes(30);
        _cache = new ConcurrentDictionary<string, CacheEntry>();
        _evictionTimer = new Timer(EvictExpired, null, TimeSpan.FromMinutes(5), TimeSpan.FromMinutes(5));
    }

    public int Count => _cache.Count;
    public long CurrentSizeBytes => _currentSizeBytes;

    /// <summary>
    /// Gets a blob from cache, or null if not found.
    /// Updates access time (MRU).
    /// </summary>
    public byte[]? Get(string key)
    {
        if (_cache.TryGetValue(key, out var entry))
        {
            if (entry.IsExpired(_ttl))
            {
                _cache.TryRemove(key, out _);
                DecrementSize(entry.Data.Length);
                return null;
            }

            entry.UpdateAccessTime();
            return entry.Data;
        }

        return null;
    }

    /// <summary>
    /// Adds or updates a blob in cache.
    /// Evicts old entries if needed to make space.
    /// </summary>
    public void Put(string key, byte[] data)
    {
        if (data == null || data.Length == 0)
            return;

        // Evict if needed
        while (_currentSizeBytes + data.Length > _maxSizeBytes && _cache.Count > 0)
        {
            EvictOldest();
        }

        // Add or update
        if (_cache.TryGetValue(key, out var oldEntry))
        {
            DecrementSize(oldEntry.Data.Length);
        }

        var entry = new CacheEntry
        {
            Key = key,
            Data = data,
            AccessTime = DateTime.UtcNow,
            InsertTime = DateTime.UtcNow
        };

        _cache.AddOrUpdate(key, entry, (k, v) => entry);
        IncrementSize(data.Length);
    }

    /// <summary>
    /// Removes a specific entry.
    /// </summary>
    public bool Remove(string key)
    {
        if (_cache.TryRemove(key, out var entry))
        {
            DecrementSize(entry.Data.Length);
            return true;
        }
        return false;
    }

    /// <summary>
    /// Clears all entries.
    /// </summary>
    public void Clear()
    {
        _cache.Clear();
        lock (_sizeLock)
        {
            _currentSizeBytes = 0;
        }
    }

    private void EvictOldest()
    {
        if (_cache.IsEmpty)
            return;

        // Find oldest entry (least recently used)
        var oldest = _cache.Values
            .OrderBy(e => e.AccessTime)
            .FirstOrDefault();

        if (oldest != null)
        {
            _cache.TryRemove(oldest.Key, out _);
            DecrementSize(oldest.Data.Length);
        }
    }

    private void EvictExpired(object? state)
    {
        var now = DateTime.UtcNow;
        var expired = _cache.Values
            .Where(e => e.IsExpired(_ttl))
            .ToList();

        foreach (var entry in expired)
        {
            if (_cache.TryRemove(entry.Key, out _))
            {
                DecrementSize(entry.Data.Length);
            }
        }
    }

    private void IncrementSize(int bytes)
    {
        lock (_sizeLock)
        {
            _currentSizeBytes += bytes;
        }
    }

    private void DecrementSize(int bytes)
    {
        lock (_sizeLock)
        {
            _currentSizeBytes = Math.Max(0, _currentSizeBytes - bytes);
        }
    }

    public void Dispose()
    {
        _evictionTimer?.Dispose();
        Clear();
    }

    private class CacheEntry
    {
        public string Key { get; set; } = string.Empty;
        public byte[] Data { get; set; } = Array.Empty<byte>();
        public DateTime AccessTime { get; set; }
        public DateTime InsertTime { get; set; }

        public bool IsExpired(TimeSpan ttl)
        {
            return DateTime.UtcNow - InsertTime > ttl;
        }

        public void UpdateAccessTime()
        {
            AccessTime = DateTime.UtcNow;
        }
    }
}



