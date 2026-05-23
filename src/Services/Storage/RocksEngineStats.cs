namespace Agent.Services.Storage;

/// <summary>
/// Combined RocksDB engine stats across the chunk DB and the bucket DB.
/// Surfaced via <see cref="RocksDbStorageService.GetEngineStats"/> and consumed
/// by the /stats/runtime endpoint.
/// </summary>
public sealed record RocksEngineStats
{
    public RocksDbEnginePerDbStats Chunk  { get; init; } = new();
    public RocksDbEnginePerDbStats Bucket { get; init; } = new();
}

/// <summary>
/// Per-DB stats slice. All bytes are <c>long</c> because RocksDB returns values
/// as decimal strings and the largest of these (TotalSstBytes) can exceed 4 GB
/// on a single agent at production scale.
///
/// Field guide:
///   - <c>TotalSstBytes</c>: full on-disk size of immutable SST files (the LSM body).
///   - <c>PendingCompactionBytes</c>: bytes the compactor *plans* to rewrite. Big
///     and growing ⇒ compaction backlog ⇒ write stalls coming.
///   - <c>CompactionPending</c>: true when at least one level needs compaction
///     scheduled. Toggling on for sustained periods is normal under heavy
///     ingest; staying on without progress means workers are starved.
///   - <c>WriteStopped</c>: hard write stall. New writes block until L0 drains.
///     If this is ever true outside of brief startup windows you have a problem.
///   - <c>MemtableBytes</c>: bytes buffered in memtables before flush. High and
///     stable ⇒ flush keeping up; high and rising ⇒ flush falling behind.
///   - <c>ImmutableMemtableCount</c>: number of memtables waiting to be flushed.
///     Anything ≥ 2 sustained ⇒ flushes are slower than ingest.
///   - <c>BlockCacheUsedBytes</c>: native LRU cache fill. Subtracts from RAM
///     budget for everything else; combine with managed-heap stats for the
///     full picture.
///   - <c>EstimateNumKeys</c>: approximate live-key count across all levels.
///     Useful for cardinality vs disk-size sanity checks.
///   - <c>NumFilesPerLevel</c>: per-LSM-level file count (L0..L6). L0 spike ⇒
///     compaction lag.
///   - <c>ActualDelayedWriteRate</c>: bytes/sec cap RocksDB is currently
///     enforcing on writes (0 = no throttling). Non-zero values are the most
///     direct measurement of "the engine is slowing me down right now".
/// </summary>
public sealed record RocksDbEnginePerDbStats
{
    public long TotalSstBytes             { get; init; }
    public long LiveSstBytes              { get; init; }
    public long PendingCompactionBytes    { get; init; }
    public bool CompactionPending         { get; init; }
    public bool WriteStopped              { get; init; }
    public long MemtableBytes             { get; init; }
    public int  ImmutableMemtableCount    { get; init; }
    public long BlockCacheUsedBytes       { get; init; }
    public long BlockCachePinnedBytes     { get; init; }
    public long TableReaderMemBytes       { get; init; }
    public long EstimateNumKeys           { get; init; }
    public int[] NumFilesPerLevel         { get; init; } = Array.Empty<int>();
    public long ActualDelayedWriteRate    { get; init; }
}
