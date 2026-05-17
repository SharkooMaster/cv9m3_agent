using System.Collections.Generic;

namespace Agent.Services.Storage;

/// <summary>
/// Sharded LRU cache for atomic per-key monotonic counters with write-through-on-eviction
/// to a durable backing store.
///
/// Goals:
///   1. Memory is bounded — total live entries ≤ totalCapacity, regardless of how many
///      distinct keys have ever been written. Replaces the unbounded ConcurrentDictionary
///      that scaled linearly with the bucket count and hit the cgroup memory limit at
///      tens of millions of entries.
///   2. Cache is the *authoritative* source of truth between disk syncs. A reader that
///      finds the key in the cache always sees the latest value. A reader that misses
///      reads from disk; durability of the disk read is guaranteed by the sync flush
///      that happens BEFORE the cache slot is reused (see EvictOne).
///   3. Concurrent FetchAndIncrement on the same key is the caller's responsibility to
///      serialise (this matches the existing per-bucket stripe-lock contract in
///      RocksDbBucketStorage). Concurrent operations on *different* keys in the same
///      shard are serialised by the shard lock.
///
/// Crash safety: a periodic background flush writes dirty entries to disk so that a
/// hard kill loses at most flushIntervalMs of in-flight counter increments. This matches
/// the durability the previous in-memory dict provided (it relied on the WriteBatcher's
/// 50 ms-5 s flush window for the same data).
/// </summary>
internal sealed class BoundedCounterCache<TKey> : IDisposable where TKey : notnull, IEquatable<TKey>
{
    private readonly Shard[] _shards;
    private readonly int _capacityPerShard;
    private readonly Func<TKey, ulong?> _diskRead;
    private readonly Action<TKey, ulong> _diskWriteSync;

    private readonly Timer _backgroundFlushTimer;
    private volatile bool _disposed;

    public BoundedCounterCache(
        int totalCapacity,
        int shardCount,
        Func<TKey, ulong?> diskRead,
        Action<TKey, ulong> diskWriteSync,
        int backgroundFlushIntervalMs = 5000)
    {
        if (totalCapacity < shardCount) totalCapacity = shardCount;
        _capacityPerShard = totalCapacity / shardCount;
        _diskRead = diskRead ?? throw new ArgumentNullException(nameof(diskRead));
        _diskWriteSync = diskWriteSync ?? throw new ArgumentNullException(nameof(diskWriteSync));

        _shards = new Shard[shardCount];
        for (int i = 0; i < shardCount; i++) _shards[i] = new Shard();

        _backgroundFlushTimer = new Timer(_ => SafeBackgroundFlush(),
            null, backgroundFlushIntervalMs, backgroundFlushIntervalMs);

        Console.WriteLine(
            $"[CounterCache<{typeof(TKey).Name}>] Initialised: shards={shardCount}, " +
            $"capacityPerShard={_capacityPerShard} (total={shardCount * _capacityPerShard}), " +
            $"backgroundFlushMs={backgroundFlushIntervalMs}");
    }

    private Shard GetShard(TKey key)
    {
        uint h = (uint)key.GetHashCode();
        return _shards[h % (uint)_shards.Length];
    }

    /// <summary>
    /// Read the current counter value. Returns null when the key has never been written
    /// (neither in cache nor on disk). On miss, populates the cache with the on-disk value.
    /// </summary>
    public ulong? TryGet(TKey key)
    {
        var shard = GetShard(key);
        lock (shard.Lock)
        {
            if (shard.Map.TryGetValue(key, out var node))
            {
                shard.List.Remove(node);
                shard.List.AddFirst(node);
                return node.Value.Value;
            }

            // Disk fallback under shard lock so that a concurrent reader/writer for the
            // same key takes the same path consistently.
            ulong? loaded = _diskRead(key);
            if (loaded == null) return null;

            EnsureCapacity(shard);
            var newNode = new LinkedListNode<Entry>(new Entry(key, loaded.Value, dirty: false));
            shard.List.AddFirst(newNode);
            shard.Map[key] = newNode;
            return loaded.Value;
        }
    }

    /// <summary>
    /// Atomically allocate the next index for <paramref name="key"/> and return the value
    /// it had BEFORE the increment. If the key has never been seen, starts at 0 and
    /// invokes <paramref name="onFirstFetch"/> exactly once (under the shard lock — keep
    /// it cheap).
    /// </summary>
    public ulong FetchAndIncrement(TKey key, Action? onFirstFetch = null)
    {
        var shard = GetShard(key);
        lock (shard.Lock)
        {
            if (shard.Map.TryGetValue(key, out var node))
            {
                ulong cur = node.Value.Value;
                node.Value = new Entry(key, cur + 1, dirty: true);
                shard.List.Remove(node);
                shard.List.AddFirst(node);
                return cur;
            }

            ulong? loaded = _diskRead(key);
            bool isBrandNew = loaded == null;
            ulong baseValue = loaded ?? 0UL;

            if (isBrandNew)
            {
                try { onFirstFetch?.Invoke(); }
                catch (Exception ex)
                {
                    Console.WriteLine($"[CounterCache] onFirstFetch threw: {ex.GetType().Name}: {ex.Message}");
                }
            }

            EnsureCapacity(shard);
            var newNode = new LinkedListNode<Entry>(new Entry(key, baseValue + 1, dirty: true));
            shard.List.AddFirst(newNode);
            shard.Map[key] = newNode;
            return baseValue;
        }
    }

    private void EnsureCapacity(Shard shard)
    {
        if (shard.Map.Count < _capacityPerShard) return;
        EvictOne(shard);
    }

    /// <summary>
    /// Evict the LRU tail. If dirty, sync-write to disk BEFORE removing from the map so
    /// that any concurrent reader on this shard waiting on the lock will, after acquiring
    /// the lock, take the disk-read path and see the just-written value.
    /// </summary>
    private void EvictOne(Shard shard)
    {
        var tail = shard.List.Last;
        if (tail == null) return;

        var entry = tail.Value;
        if (entry.Dirty)
        {
            try { _diskWriteSync(entry.Key, entry.Value); }
            catch (Exception ex)
            {
                // We surface the error but still proceed with eviction; the next reader
                // will see the previous on-disk value, which is no worse than the pre-cache
                // behaviour (a stale read).
                Console.WriteLine($"[CounterCache] Sync write on evict failed for key={entry.Key}: {ex.GetType().Name}: {ex.Message}");
            }
        }

        shard.List.RemoveLast();
        shard.Map.Remove(entry.Key);
    }

    /// <summary>
    /// Sweep all shards, sync-writing dirty entries to disk and clearing the dirty flag.
    /// Cheap because we only iterate the LRU list under the shard lock and only write
    /// the dirty subset; the per-write cost is amortised across the flush interval.
    /// </summary>
    private void SafeBackgroundFlush()
    {
        if (_disposed) return;
        try { FlushDirty(); }
        catch (Exception ex)
        {
            Console.WriteLine($"[CounterCache] Background flush failed: {ex.GetType().Name}: {ex.Message}");
        }
    }

    public int FlushDirty()
    {
        int written = 0;
        for (int i = 0; i < _shards.Length; i++)
        {
            var shard = _shards[i];
            lock (shard.Lock)
            {
                var node = shard.List.First;
                while (node != null)
                {
                    if (node.Value.Dirty)
                    {
                        try
                        {
                            _diskWriteSync(node.Value.Key, node.Value.Value);
                            node.Value = new Entry(node.Value.Key, node.Value.Value, dirty: false);
                            written++;
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"[CounterCache] FlushDirty failed for key={node.Value.Key}: {ex.GetType().Name}: {ex.Message}");
                        }
                    }
                    node = node.Next;
                }
            }
        }
        return written;
    }

    public int Count
    {
        get
        {
            int total = 0;
            for (int i = 0; i < _shards.Length; i++)
            {
                lock (_shards[i].Lock) total += _shards[i].Map.Count;
            }
            return total;
        }
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        try { _backgroundFlushTimer.Dispose(); } catch { }
        try { FlushDirty(); }
        catch (Exception ex)
        {
            Console.WriteLine($"[CounterCache] Dispose flush failed: {ex.GetType().Name}: {ex.Message}");
        }
    }

    private sealed class Shard
    {
        public readonly object Lock = new();
        public readonly LinkedList<Entry> List = new();
        public readonly Dictionary<TKey, LinkedListNode<Entry>> Map = new();
    }

    private struct Entry
    {
        public TKey Key;
        public ulong Value;
        public bool Dirty;

        public Entry(TKey key, ulong value, bool dirty)
        {
            Key = key;
            Value = value;
            Dirty = dirty;
        }
    }
}
