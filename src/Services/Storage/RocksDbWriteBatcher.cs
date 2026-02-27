using System.Collections.Concurrent;
using System.Text;
using RocksDbSharp;

namespace Agent.Services.Storage;

/// <summary>
/// Batches RocksDB writes for improved performance.
/// Flushes automatically every N operations or every X milliseconds.
/// Failed writes are retried up to MaxRetries before being dropped.
/// The blocking Flush() path (called by StoreVector RPCs) propagates errors
/// so callers know data was NOT persisted — preventing silent data loss.
/// </summary>
public sealed class RocksDbWriteBatcher : IDisposable
{
    private readonly RocksDb _rocksDb;
    private readonly ConcurrentQueue<(byte[] key, byte[] value, int retryCount)> _writeQueue = new();
    private readonly Timer _flushTimer;
    private readonly int _batchSize;
    private readonly int _flushIntervalMs;
    private readonly object _flushLock = new();
    private volatile bool _disposed = false;

    private const int MaxRetries = 3;

    public RocksDbWriteBatcher(RocksDb rocksDb, int batchSize = 500, int flushIntervalMs = 50)
    {
        _rocksDb = rocksDb ?? throw new ArgumentNullException(nameof(rocksDb));
        _batchSize = batchSize;
        _flushIntervalMs = flushIntervalMs;
        
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
        
        _writeQueue.Enqueue((key, value, 0));
        
        if (_writeQueue.Count >= _batchSize)
        {
            _ = Task.Run(() => FlushBatch());
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
    /// Flush all pending writes immediately. BLOCKING — waits for any in-progress flush,
    /// then drains the entire queue. After this returns, all queued data is in RocksDB WAL.
    /// Called by StoreVectorService before responding to RPCs (crash safety guarantee).
    /// THROWS on persistent write failure so the RPC can return an error to Cross.
    /// </summary>
    public void Flush()
    {
        FlushBlocking(propagateErrors: true);
    }

    /// <summary>
    /// Blocking flush: acquires the lock (waits if needed), drains ALL pending items.
    /// When propagateErrors=true (RPC path), throws on write failure so the caller
    /// knows data was NOT persisted and can respond with an error.
    /// When propagateErrors=false (Dispose/timer path), re-queues with retry cap.
    /// </summary>
    private void FlushBlocking(bool propagateErrors = false)
    {
        if (_disposed || _writeQueue.IsEmpty)
            return;

        Monitor.Enter(_flushLock);
        try
        {
            while (!_writeQueue.IsEmpty)
            {
                var batch = new WriteBatch();
                var items = new List<(byte[] key, byte[] value, int retryCount)>();
                int count = 0;

                while (_writeQueue.TryDequeue(out var item) && count < 10_000)
                {
                    batch.Put(item.key, item.value);
                    items.Add(item);
                    count++;
                }

                if (count > 0)
                {
                    try
                    {
                        _rocksDb.Write(batch);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[RocksDB WriteBatcher] WRITE FAILED ({count} items): {ex.Message}");
                        batch.Dispose();

                        if (propagateErrors)
                            throw new IOException($"RocksDB write failed for {count} items — data NOT persisted", ex);

                        int requeued = 0, dropped = 0;
                        foreach (var item in items)
                        {
                            if (item.retryCount < MaxRetries)
                            {
                                _writeQueue.Enqueue((item.key, item.value, item.retryCount + 1));
                                requeued++;
                            }
                            else
                            {
                                dropped++;
                            }
                        }
                        if (dropped > 0)
                            Console.WriteLine($"[RocksDB WriteBatcher] DROPPED {dropped} items after {MaxRetries} retries (DATA LOSS)");
                        if (requeued > 0)
                            Console.WriteLine($"[RocksDB WriteBatcher] Re-queued {requeued} items for retry");
                        return;
                    }
                    finally
                    {
                        batch.Dispose();
                    }
                }
            }
        }
        finally
        {
            Monitor.Exit(_flushLock);
        }
    }

    /// <summary>
    /// Timer-based background flush: non-blocking, skips if another flush is in progress.
    /// Re-queues failed items with a retry counter instead of dropping them.
    /// </summary>
    private void FlushBatch()
    {
        if (_disposed || _writeQueue.IsEmpty)
            return;

        if (!Monitor.TryEnter(_flushLock, 0))
            return;

        try
        {
            if (_writeQueue.IsEmpty)
                return;

            var batch = new WriteBatch();
            var items = new List<(byte[] key, byte[] value, int retryCount)>();
            int count = 0;

            while (_writeQueue.TryDequeue(out var item) && count < _batchSize * 2)
            {
                batch.Put(item.key, item.value);
                items.Add(item);
                count++;
            }

            if (count > 0)
            {
                try
                {
                    _rocksDb.Write(batch);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[RocksDB WriteBatcher] Background flush FAILED ({count} items): {ex.Message}");
                    int requeued = 0, dropped = 0;
                    foreach (var item in items)
                    {
                        if (item.retryCount < MaxRetries)
                        {
                            _writeQueue.Enqueue((item.key, item.value, item.retryCount + 1));
                            requeued++;
                        }
                        else
                        {
                            dropped++;
                        }
                    }
                    if (dropped > 0)
                        Console.WriteLine($"[RocksDB WriteBatcher] DROPPED {dropped} items after {MaxRetries} retries (DATA LOSS)");
                    if (requeued > 0)
                        Console.WriteLine($"[RocksDB WriteBatcher] Re-queued {requeued} items for retry");
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

        _flushTimer?.Dispose();

        // Final flush — best-effort, don't propagate errors during shutdown
        FlushBlocking(propagateErrors: false);

        _disposed = true;
    }
}
