using System.Collections.Concurrent;

namespace Agent.Services.Cache;

/// <summary>
/// LRU blob cache with size and time-based eviction.
/// Uses a doubly-linked list for O(1) eviction of the least recently used entry.
/// Thread-safe via a dedicated lock for the LRU list + size tracking.
/// </summary>
public class MruBlobCache : IDisposable
{
    private readonly ConcurrentDictionary<string, LinkedListNode<CacheEntry>> _cache;
    private readonly LinkedList<CacheEntry> _lruList; // Head = most recent, Tail = least recent
    private readonly long _maxSizeBytes;
    private readonly TimeSpan _ttl;
    private readonly Timer _evictionTimer;
    private long _currentSizeBytes;
    private readonly object _lruLock = new(); // protects _lruList and _currentSizeBytes

    public MruBlobCache(
        long maxSizeBytes = 1024 * 1024 * 1024, // 1GB default
        TimeSpan? ttl = null)
    {
        _maxSizeBytes = maxSizeBytes;
        _ttl = ttl ?? TimeSpan.FromMinutes(30);
        _cache = new ConcurrentDictionary<string, LinkedListNode<CacheEntry>>();
        _lruList = new LinkedList<CacheEntry>();
        _evictionTimer = new Timer(EvictExpired, null, TimeSpan.FromMinutes(5), TimeSpan.FromMinutes(5));
    }

    public int Count => _cache.Count;
    public long CurrentSizeBytes => Interlocked.Read(ref _currentSizeBytes);

    /// <summary>
    /// Gets a blob from cache, or null if not found.
    /// Promotes to head (most recently used) on hit.
    /// </summary>
    public byte[]? Get(string key)
    {
        if (string.IsNullOrEmpty(key))
            return null;

        if (_cache.TryGetValue(key, out var node))
        {
            lock (_lruLock)
            {
                if (node.List == null) return null; // removed concurrently
                if (node.Value.IsExpired(_ttl))
                {
                    _lruList.Remove(node);
                    _cache.TryRemove(key, out _);
                    Interlocked.Add(ref _currentSizeBytes, -node.Value.Data.Length);
                    return null;
                }
                // Promote to head (most recently used)
                _lruList.Remove(node);
                _lruList.AddFirst(node);
                node.Value.AccessTime = DateTime.UtcNow;
            }
            return node.Value.Data;
        }

        return null;
    }

    /// <summary>
    /// Adds or updates a blob in cache.
    /// O(1) eviction of LRU entries when over budget.
    /// </summary>
    public void Put(string key, byte[] data)
    {
        if (string.IsNullOrEmpty(key) || data == null || data.Length == 0)
            return;

        lock (_lruLock)
        {
            // If key already exists, remove the old entry first
            if (_cache.TryRemove(key, out var oldNode))
            {
                _lruList.Remove(oldNode);
                Interlocked.Add(ref _currentSizeBytes, -oldNode.Value.Data.Length);
            }

            // Evict LRU entries until we have space — O(1) per eviction
            while (Interlocked.Read(ref _currentSizeBytes) + data.Length > _maxSizeBytes && _lruList.Count > 0)
            {
                var tail = _lruList.Last;
                if (tail == null) break;
                _lruList.RemoveLast();
                _cache.TryRemove(tail.Value.Key, out _);
                Interlocked.Add(ref _currentSizeBytes, -tail.Value.Data.Length);
            }

            // Add new entry at head (most recently used)
            var entry = new CacheEntry
            {
                Key = key,
                Data = data,
                AccessTime = DateTime.UtcNow,
                InsertTime = DateTime.UtcNow
            };
            var newNode = _lruList.AddFirst(entry);
            _cache[key] = newNode;
            Interlocked.Add(ref _currentSizeBytes, data.Length);
        }
    }

    /// <summary>
    /// Removes a specific entry.
    /// </summary>
    public bool Remove(string key)
    {
        if (_cache.TryRemove(key, out var node))
        {
            lock (_lruLock)
            {
                if (node.List != null)
                    _lruList.Remove(node);
                Interlocked.Add(ref _currentSizeBytes, -node.Value.Data.Length);
            }
            return true;
        }
        return false;
    }

    /// <summary>
    /// Clears all entries.
    /// </summary>
    public void Clear()
    {
        lock (_lruLock)
        {
            _cache.Clear();
            _lruList.Clear();
            Interlocked.Exchange(ref _currentSizeBytes, 0);
        }
    }

    /// <summary>
    /// Timer callback: evict entries that have exceeded TTL.
    /// </summary>
    private void EvictExpired(object? state)
    {
        lock (_lruLock)
        {
            // Walk from tail (oldest) toward head, removing expired entries
            var node = _lruList.Last;
            int evicted = 0;
            while (node != null)
            {
                var prev = node.Previous;
                if (node.Value.IsExpired(_ttl))
                {
                    _lruList.Remove(node);
                    _cache.TryRemove(node.Value.Key, out _);
                    Interlocked.Add(ref _currentSizeBytes, -node.Value.Data.Length);
                    evicted++;
                }
                node = prev;
            }
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
    }
}
