using System.Collections.Concurrent;
using System.Text;
using System.Text.Json;
using RocksDbSharp;

namespace Agent.Services.Storage;

/// <summary>
/// Batches RocksDB bucket writes for improved performance.
/// Handles read-modify-write operations atomically.
/// </summary>
public sealed class RocksDbBucketWriteBatcher : IDisposable
{
    private readonly RocksDb _rocksDb;
    private readonly ConcurrentDictionary<string, BucketUpdate> _pendingUpdates = new();
    private readonly Timer _flushTimer;
    private readonly int _flushIntervalMs;
    private readonly object _flushLock = new();
    private volatile bool _disposed = false;

    public RocksDbBucketWriteBatcher(RocksDb rocksDb, int flushIntervalMs = 100)
    {
        _rocksDb = rocksDb ?? throw new ArgumentNullException(nameof(rocksDb));
        _flushIntervalMs = flushIntervalMs;
        
        // Flush periodically
        _flushTimer = new Timer(_ => FlushBatch(), null, flushIntervalMs, flushIntervalMs);
        
        Console.WriteLine($"[RocksDB BucketWriteBatcher] Initialized: flushInterval={flushIntervalMs}ms");
    }

    /// <summary>
    /// Queue a bucket update. Returns immediately (non-blocking).
    /// </summary>
    public void QueueUpdate(string bucketName, Func<BucketData, BucketData> updateFunc)
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(RocksDbBucketWriteBatcher));
        
        _pendingUpdates.AddOrUpdate(
            bucketName,
            new BucketUpdate { UpdateFunc = updateFunc },
            (key, existing) => new BucketUpdate { UpdateFunc = updateFunc }
        );
    }

    /// <summary>
    /// Flush all pending updates immediately. Thread-safe.
    /// </summary>
    public void Flush()
    {
        FlushBatch();
    }

    private void FlushBatch()
    {
        if (_disposed || _pendingUpdates.IsEmpty)
            return;

        lock (_flushLock)
        {
            if (_pendingUpdates.IsEmpty)
                return;

            var batch = new WriteBatch();
            var updatesToProcess = new Dictionary<string, BucketUpdate>();
            
            // Snapshot pending updates
            foreach (var kvp in _pendingUpdates)
            {
                updatesToProcess[kvp.Key] = kvp.Value;
            }
            _pendingUpdates.Clear();

            int count = 0;
            foreach (var kvp in updatesToProcess)
            {
                try
                {
                    var bucketName = kvp.Key;
                    var update = kvp.Value;
                    var key = Encoding.UTF8.GetBytes($"bucket:{bucketName}");
                    
                    // Read current value
                    var existingJson = _rocksDb.Get(key);
                    BucketData bucketData;
                    
                    if (existingJson != null && existingJson.Length > 0)
                    {
                        try
                        {
                            bucketData = JsonSerializer.Deserialize<BucketData>(Encoding.UTF8.GetString(existingJson)) ?? new BucketData();
                        }
                        catch
                        {
                            bucketData = new BucketData();
                        }
                    }
                    else
                    {
                        bucketData = new BucketData();
                    }

                    // Apply update
                    var updatedData = update.UpdateFunc(bucketData);
                    
                    // Write back
                    var json = JsonSerializer.Serialize(updatedData);
                    batch.Put(key, Encoding.UTF8.GetBytes(json));
                    count++;
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[RocksDB BucketWriteBatcher] Error processing update for {kvp.Key}: {ex.Message}");
                    // Re-queue failed update
                    _pendingUpdates.TryAdd(kvp.Key, kvp.Value);
                }
            }

            if (count > 0)
            {
                try
                {
                    _rocksDb.Write(batch);
                    // Console.WriteLine($"[RocksDB BucketWriteBatcher] Flushed {count} bucket updates");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[RocksDB BucketWriteBatcher] Error flushing batch: {ex.Message}");
                    // Re-queue all failed updates
                    foreach (var kvp in updatesToProcess)
                    {
                        _pendingUpdates.TryAdd(kvp.Key, kvp.Value);
                    }
                }
                finally
                {
                    batch.Dispose();
                }
            }
        }
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;
        _flushTimer?.Dispose();
        
        // Final flush on shutdown
        FlushBatch();
        
        // Wait a bit for final flush to complete
        Thread.Sleep(50);
    }

    private class BucketUpdate
    {
        public Func<BucketData, BucketData> UpdateFunc { get; set; } = null!;
    }

    public class BucketData
    {
        public ulong BucketId { get; set; }
        public List<VectorData> Vectors { get; set; } = new();
    }

    public class VectorData
    {
        public float[] Vector { get; set; } = Array.Empty<float>();
        public string StorageGuid { get; set; } = string.Empty;
        public int ChunkSize { get; set; }
    }
}
