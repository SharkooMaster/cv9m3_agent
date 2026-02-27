using System.Collections.Concurrent;

namespace Agent.Services.Cache;

/// <summary>
/// LRU blob cache with high-water/low-water background eviction.
///
/// Uses a doubly-linked list for O(1) eviction of the least recently used entry.
/// Eviction model (non-blocking):
///   - High-water (90%): signals dedicated background thread to start evicting.
///   - Low-water (75%): eviction stops, creating ~15% headroom for burst writes.
///   - Hard ceiling (98%): inline blocking eviction during Put() — emergency only.
///
/// Thread-safe: _lruLock protects the linked list and size tracking.
/// The background evictor acquires the lock in small batches to avoid starving hot-path ops.
/// </summary>
public class MruBlobCache : IDisposable
{
    private readonly ConcurrentDictionary<string, LinkedListNode<CacheEntry>> _cache;
    private readonly LinkedList<CacheEntry> _lruList; // Head = most recent, Tail = least recent
    private readonly long _maxSizeBytes;
    private readonly TimeSpan _ttl;
    private readonly Timer _ttlTimer;
    private long _currentSizeBytes;
    private readonly object _lruLock = new(); // protects _lruList and _currentSizeBytes

    // ── Watermark thresholds ──
    private readonly long _highWaterBytes;
    private readonly long _lowWaterBytes;
    private readonly long _hardCeilingBytes;

    // ── Background evictor thread ──
    private readonly Thread _evictorThread;
    private readonly ManualResetEventSlim _evictSignal = new(false);
    private volatile bool _disposed;

    private const int EvictBatchSize = 200;

    public MruBlobCache(
        long maxSizeBytes = 1024 * 1024 * 1024, // 1GB default
        TimeSpan? ttl = null,
        double highWaterPct = 0.70,
        double lowWaterPct = 0.50,
        double hardCeilingPct = 0.90)
    {
        _maxSizeBytes = maxSizeBytes;
        _ttl = ttl ?? TimeSpan.FromMinutes(30);
        _cache = new ConcurrentDictionary<string, LinkedListNode<CacheEntry>>();
        _lruList = new LinkedList<CacheEntry>();

        _highWaterBytes = (long)(maxSizeBytes * highWaterPct);
        _lowWaterBytes = (long)(maxSizeBytes * lowWaterPct);
        _hardCeilingBytes = (long)(maxSizeBytes * hardCeilingPct);

        // TTL expiry timer (runs every 5 minutes to clean old entries)
        _ttlTimer = new Timer(EvictExpired, null, TimeSpan.FromMinutes(5), TimeSpan.FromMinutes(5));

        // Background evictor thread
        _evictorThread = new Thread(BackgroundEvictorLoop)
        {
            IsBackground = true,
            Name = "ChunkCache-Evictor",
            Priority = ThreadPriority.BelowNormal
        };
        _evictorThread.Start();
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
    /// Signals background eviction if over high-water mark.
    /// Emergency inline eviction at hard ceiling only.
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

            // Hard ceiling (98%): emergency inline eviction — evict just enough to fit
            long afterAdd = Interlocked.Read(ref _currentSizeBytes) + data.Length;
            if (afterAdd > _hardCeilingBytes)
            {
                int inlineEvicted = 0;
                while (Interlocked.Read(ref _currentSizeBytes) + data.Length > _lowWaterBytes && _lruList.Count > 0)
                {
                    var tail = _lruList.Last;
                    if (tail == null) break;
                    _lruList.RemoveLast();
                    _cache.TryRemove(tail.Value.Key, out _);
                    Interlocked.Add(ref _currentSizeBytes, -tail.Value.Data.Length);
                    inlineEvicted++;
                }
                if (inlineEvicted > 100) // only log if significant
                    Console.WriteLine($"[ChunkCache] ⚠️ Hard ceiling inline eviction: removed {inlineEvicted} entries");
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

        // Signal background evictor if over high-water (non-blocking)
        if (Interlocked.Read(ref _currentSizeBytes) > _highWaterBytes)
            _evictSignal.Set();
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
    /// Force-evict down to a target percentage of max capacity.
    /// Called by the system memory guard when node memory is critically low.
    /// </summary>
    /// <param name="keepPct">Fraction of max capacity to keep (0.0 = clear all, 0.5 = keep 50%).</param>
    public void ForceEvict(double keepPct = 0.0)
    {
        long target = (long)(_maxSizeBytes * Math.Clamp(keepPct, 0.0, 1.0));
        long currentSize = Interlocked.Read(ref _currentSizeBytes);
        if (currentSize <= target) return;

        int evicted = 0;
        lock (_lruLock)
        {
            while (Interlocked.Read(ref _currentSizeBytes) > target && _lruList.Count > 0)
            {
                var tail = _lruList.Last;
                if (tail == null) break;
                _lruList.RemoveLast();
                _cache.TryRemove(tail.Value.Key, out _);
                Interlocked.Add(ref _currentSizeBytes, -tail.Value.Data.Length);
                evicted++;
            }
        }
        if (evicted > 0)
        {
            long afterSize = Interlocked.Read(ref _currentSizeBytes);
            Console.WriteLine(
                $"[ChunkCache] [SYSMEM] Force-evicted {evicted} entries. " +
                $"Usage: {afterSize / (1024 * 1024)}MB / {_maxSizeBytes / (1024 * 1024)}MB");
        }
    }

    // ═══════════════════════════════════════════════════════════════════
    //  BACKGROUND EVICTOR
    // ═══════════════════════════════════════════════════════════════════

    /// <summary>
    /// Dedicated background thread: waits for signal, evicts down to low-water in batches.
    /// </summary>
    private void BackgroundEvictorLoop()
    {
        while (!_disposed)
        {
            _evictSignal.Wait(TimeSpan.FromSeconds(5));
            _evictSignal.Reset();

            if (_disposed) break;

            long currentSize = Interlocked.Read(ref _currentSizeBytes);
            if (currentSize <= _highWaterBytes) continue;

            long target = _lowWaterBytes;
            int totalEvicted = 0;
            long totalFreed = 0;

            // Evict in batches, yielding between batches
            while (Interlocked.Read(ref _currentSizeBytes) > target && !_disposed)
            {
                int batchEvicted = 0;
                long batchFreed = 0;

                lock (_lruLock)
                {
                    for (int i = 0; i < EvictBatchSize && _lruList.Count > 0; i++)
                    {
                        long curSize = Interlocked.Read(ref _currentSizeBytes);
                        if (curSize <= target) break;

                        var tail = _lruList.Last;
                        if (tail == null) break;
                        _lruList.RemoveLast();
                        _cache.TryRemove(tail.Value.Key, out _);
                        long entrySize = tail.Value.Data.Length;
                        Interlocked.Add(ref _currentSizeBytes, -entrySize);
                        batchEvicted++;
                        batchFreed += entrySize;
                    }
                }

                totalEvicted += batchEvicted;
                totalFreed += batchFreed;

                if (batchEvicted == 0) break; // nothing left to evict

                // Yield between batches to let Put()/Get() proceed
                Thread.Yield();
            }

            if (totalEvicted > 0)
            {
                long afterSize = Interlocked.Read(ref _currentSizeBytes);
                Console.WriteLine(
                    $"[ChunkCache] [bg] Evicted {totalEvicted} entries, freed ~{totalFreed / (1024 * 1024)}MB. " +
                    $"Usage: {afterSize / (1024 * 1024)}MB / {_maxSizeBytes / (1024 * 1024)}MB ({afterSize * 100 / Math.Max(1, _maxSizeBytes)}%)");
            }
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
        _disposed = true;
        _evictSignal.Set(); // wake up background thread so it exits
        _evictorThread?.Join(TimeSpan.FromSeconds(3));
        _ttlTimer?.Dispose();
        _evictSignal?.Dispose();
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
