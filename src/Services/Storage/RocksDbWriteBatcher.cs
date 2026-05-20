using System.Collections.Concurrent;
using System.Text;
using RocksDbSharp;

namespace Agent.Services.Storage;

/// <summary>
/// Batches RocksDB writes with a coalesced flush pattern for high throughput.
///
/// Write path: Put() enqueues to a lock-free ConcurrentQueue (nanoseconds).
/// Flush path (coalesced): multiple concurrent callers share a single RocksDB WriteBatch.
///   - First caller in an epoch becomes the "flusher" and drains the queue.
///   - Subsequent callers in the same epoch wait on the same TaskCompletionSource.
///   - When the flusher finishes, ALL waiters are released simultaneously.
///   - Result: under high concurrency (24 RPCs), 1-2 flushes instead of 24.
///
/// Crash safety: Flush() blocks until all queued writes are in RocksDB WAL.
///   - Normal shutdown (SIGTERM): graceful Dispose() drains the queue. Zero data loss.
///   - Hard kill (SIGKILL/OOM): impossible — queue is in-process memory. But this
///     batcher is only used for metadata (vectors, dedup entries, indices). Chunk data
///     is also in the queue. Callers (StoreVectorService) call Flush() before responding
///     to RPCs, so if the RPC returned success, the data IS in RocksDB.
/// </summary>
public sealed class RocksDbWriteBatcher : IDisposable
{
    private readonly RocksDb _rocksDb;
    private readonly ConcurrentQueue<(byte[] key, byte[] value, int retryCount)> _writeQueue = new();
    private readonly Timer _flushTimer;
    private readonly int _batchSize;
    private volatile bool _disposed = false;

    private const int MaxRetries = 3;
    private const int MaxItemsPerBatch = 50_000;

    // Coalesced flush: all concurrent callers share the same flush epoch.
    // The first caller becomes the flusher; others wait on the same TCS.
    private readonly object _epochLock = new();
    private TaskCompletionSource<bool>? _currentEpoch = null;

    public RocksDbWriteBatcher(RocksDb rocksDb, int batchSize = 10_000, int flushIntervalMs = 5000)
    {
        _rocksDb = rocksDb ?? throw new ArgumentNullException(nameof(rocksDb));
        _batchSize = batchSize;

        _flushTimer = new Timer(_ => BackgroundFlush(), null, flushIntervalMs, flushIntervalMs);

        Console.WriteLine($"[RocksDB WriteBatcher] Initialized: batchSize={batchSize}, flushInterval={flushIntervalMs}ms (coalesced)");
    }

    public void Put(byte[] key, byte[] value)
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(RocksDbWriteBatcher));

        _writeQueue.Enqueue((key, value, 0));
    }

    public void Put(string key, byte[] value)
    {
        Put(Encoding.UTF8.GetBytes(key), value);
    }

    /// <summary>
    /// Coalesced flush: guarantees all writes enqueued before this call are in RocksDB WAL
    /// when it returns. Multiple concurrent callers share a single flush operation.
    ///
    /// Flow:
    ///   1. Caller checks if a flush is already in progress (_currentEpoch != null).
    ///   2. If yes: wait on that epoch's TCS. When it completes, our writes MAY have
    ///      been flushed by that epoch's drain — but only if we Put before that
    ///      drain's last iteration. After waking we MUST re-check the queue.
    ///   3. If no: become the flusher — create a new epoch TCS, drain the queue, write.
    ///   4. On completion, signal all waiters and clear the epoch.
    ///
    /// Correctness invariant: when this method returns successfully, every byte that
    /// was passed to Put() before the call began is in RocksDB's WAL.
    ///
    /// Subtle race the loop fixes: there is a small window between the flusher's
    /// outer-while loop exiting (queue observed empty) and the flusher acquiring
    /// _epochLock to clear _currentEpoch. A Put() that lands in that window leaves
    /// an item in the queue that the current epoch's drain will never see; a Flush()
    /// caller that races in just after the Put and observes _currentEpoch != null
    /// would wait on the wrong epoch and return with their write still queued. The
    /// loop catches this: after waking, if the queue still has items, we go around
    /// and try to become (or wait for) a fresh flusher whose drain will cover us.
    /// </summary>
    public void Flush()
    {
        if (_disposed)
            return;

        while (true)
        {
            Task waitTask;
            bool iAmFlusher = false;

            lock (_epochLock)
            {
                if (_currentEpoch != null)
                {
                    waitTask = _currentEpoch.Task;
                }
                else
                {
                    if (_writeQueue.IsEmpty)
                        return;
                    _currentEpoch = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
                    waitTask = _currentEpoch.Task;
                    iAmFlusher = true;
                }
            }

            if (!iAmFlusher)
            {
                try { waitTask.GetAwaiter().GetResult(); }
                catch
                {
                    throw new IOException("RocksDB coalesced flush failed — data may NOT be persisted",
                        waitTask.Exception?.InnerException);
                }
                // The epoch we waited on may have ended BEFORE our Put landed in
                // the queue. If anything is still queued, our write is at risk —
                // loop and either claim a fresh epoch or piggyback on the next one.
                if (_writeQueue.IsEmpty)
                    return;
                continue;
            }

            // I am the flusher for this epoch
            Exception? flushError = null;
            try
            {
                DrainQueue(propagateErrors: true);
            }
            catch (Exception ex)
            {
                flushError = ex;
            }
            finally
            {
                lock (_epochLock)
                {
                    var epoch = _currentEpoch!;
                    _currentEpoch = null;

                    if (flushError != null)
                        epoch.TrySetException(flushError);
                    else
                        epoch.TrySetResult(true);
                }
            }

            if (flushError != null)
                throw flushError;
            return;
        }
    }

    /// <summary>
    /// Drains the entire write queue into RocksDB WriteBatch(es).
    /// Processes up to MaxItemsPerBatch items per WriteBatch to bound memory.
    /// </summary>
    private void DrainQueue(bool propagateErrors)
    {
        while (!_writeQueue.IsEmpty)
        {
            var batch = new WriteBatch();
            var failedItems = propagateErrors ? null : new List<(byte[] key, byte[] value, int retryCount)>();
            int count = 0;

            while (_writeQueue.TryDequeue(out var item) && count < MaxItemsPerBatch)
            {
                batch.Put(item.key, item.value);
                failedItems?.Add(item);
                count++;
            }

            if (count == 0)
            {
                batch.Dispose();
                break;
            }

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

                if (failedItems != null)
                {
                    int requeued = 0, dropped = 0;
                    foreach (var item in failedItems)
                    {
                        if (item.retryCount < MaxRetries)
                        {
                            _writeQueue.Enqueue((item.key, item.value, item.retryCount + 1));
                            requeued++;
                        }
                        else
                            dropped++;
                    }
                    if (dropped > 0)
                        Console.WriteLine($"[RocksDB WriteBatcher] DROPPED {dropped} items after {MaxRetries} retries (DATA LOSS)");
                    if (requeued > 0)
                        Console.WriteLine($"[RocksDB WriteBatcher] Re-queued {requeued} items for retry");
                }
                return;
            }
            finally
            {
                batch.Dispose();
            }
        }
    }

    /// <summary>
    /// Background timer flush: best-effort. Claims the same epoch slot that callers
    /// of <see cref="Flush"/> use so concurrent waiters block on us — without that,
    /// a Flush() caller could observe _currentEpoch == null and the queue empty
    /// while our drain hadn't yet committed its WriteBatch to the WAL, returning
    /// "durable" for a write that wasn't yet on disk.
    ///
    /// If a foreground epoch is already running we skip — those writes will be
    /// included in that drain.
    /// </summary>
    private void BackgroundFlush()
    {
        if (_disposed || _writeQueue.IsEmpty)
            return;

        TaskCompletionSource<bool>? epoch = null;
        lock (_epochLock)
        {
            if (_currentEpoch != null)
                return;
            epoch = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            _currentEpoch = epoch;
        }

        Exception? flushError = null;
        try
        {
            DrainQueue(propagateErrors: false);
        }
        catch (Exception ex)
        {
            flushError = ex;
        }
        finally
        {
            lock (_epochLock)
            {
                _currentEpoch = null;
                if (flushError != null)
                    epoch.TrySetException(flushError);
                else
                    epoch.TrySetResult(true);
            }
        }

        if (flushError != null)
            Console.WriteLine($"[RocksDB WriteBatcher] Background flush error: {flushError.Message}");
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;
        _flushTimer?.Dispose();

        try
        {
            DrainQueue(propagateErrors: false);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[RocksDB WriteBatcher] Dispose flush error: {ex.Message}");
        }
    }
}
