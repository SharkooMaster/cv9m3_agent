using System.Collections.Concurrent;
using System.Text;
using RocksDbSharp;

namespace Agent.Services.Storage;

/// <summary>
/// Batches RocksDB writes for improved performance.
/// Flushes automatically every N operations or every X milliseconds.
/// </summary>
public sealed class RocksDbWriteBatcher : IDisposable
{
    private readonly RocksDb _rocksDb;
    private readonly ConcurrentQueue<(byte[] key, byte[] value)> _writeQueue = new();
    private readonly Timer _flushTimer;
    private readonly int _batchSize;
    private readonly int _flushIntervalMs;
    private readonly object _flushLock = new();
    private volatile bool _disposed = false;

    public RocksDbWriteBatcher(RocksDb rocksDb, int batchSize = 500, int flushIntervalMs = 50)
    {
        _rocksDb = rocksDb ?? throw new ArgumentNullException(nameof(rocksDb));
        _batchSize = batchSize;
        _flushIntervalMs = flushIntervalMs;
        
        // Flush periodically
        _flushTimer = new Timer(_ => FlushBatch(), null, flushIntervalMs, flushIntervalMs);
        
        Console.WriteLine($"[RocksDB WriteBatcher] Initialized: batchSize={batchSize}, flushInterval={flushIntervalMs}ms");
    }

    /// <summary>
    /// Queue a write operation. Returns immediately (non-blocking).
    /// Write happens asynchronously in the background.
    /// </summary>
    public void Put(byte[] key, byte[] value)
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(RocksDbWriteBatcher));
        
        _writeQueue.Enqueue((key, value));
        
        // Trigger background flush if batch size reached (non-blocking)
        if (_writeQueue.Count >= _batchSize)
        {
            _ = Task.Run(() => FlushBatch()); // Fire and forget - runs in background
        }
    }

    /// <summary>
    /// Queue a write operation using string key.
    /// </summary>
    public void Put(string key, byte[] value)
    {
        Put(Encoding.UTF8.GetBytes(key), value);
    }

    /// <summary>
    /// Flush all pending writes immediately. Thread-safe.
    /// </summary>
    public void Flush()
    {
        FlushBatch();
    }

    private void FlushBatch()
    {
        if (_disposed || _writeQueue.IsEmpty)
            return;

        // Try to acquire lock, but don't block if another flush is in progress
        if (!Monitor.TryEnter(_flushLock, 0))
            return; // Another flush is already running, skip this one

        try
        {
            if (_writeQueue.IsEmpty)
                return;

            var batch = new WriteBatch();
            int count = 0;
            var itemsToWrite = new List<(byte[] key, byte[] value)>();

            // Drain queue into batch (limit to prevent huge batches)
            while (_writeQueue.TryDequeue(out var item) && count < _batchSize * 2)
            {
                batch.Put(item.key, item.value);
                itemsToWrite.Add(item);
                count++;
            }

            if (count > 0)
            {
                try
                {
                    // Write batch atomically (fast operation, runs in background)
                    _rocksDb.Write(batch);
                    // Console.WriteLine($"[RocksDB WriteBatcher] Flushed {count} writes");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[RocksDB WriteBatcher] Error flushing batch: {ex.Message}");
                    // Re-queue failed writes for retry
                    foreach (var item in itemsToWrite)
                    {
                        _writeQueue.Enqueue(item);
                    }
                }
                finally
                {
                    batch.Dispose();
                }
            }
        }
        finally
        {
            Monitor.Exit(_flushLock);
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
}
