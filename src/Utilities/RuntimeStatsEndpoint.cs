using System.Threading;
using Agent.Interfaces.Infs;
using Agent.Services.Storage;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;

namespace Agent.Utils;

/// <summary>
/// Exposes <c>GET /stats/runtime</c> on this pod's HTTP/1 port (5001).
///
/// Pulled by the control-center pod every ~30 s. The handler is allocation-bounded
/// and never touches the compression / RocksDB hot path:
///   - <see cref="System.GC.GetGCMemoryInfo()"/> is ~1 µs and lock-free.
///   - <see cref="System.Diagnostics.Process.WorkingSet64"/> reads <c>/proc/self/status</c>
///     on Linux (~10–100 µs).
///   - RocksDB <c>GetProperty</c> reads in-memory counters and is non-blocking.
///
/// The shape mirrors what the control-center scraper deserialises into
/// (see <c>RuntimeStatsStore.RuntimeSample</c>) and must stay in sync with the
/// cross / gateway endpoints — adding new fields is fine, renaming or
/// repurposing is not.
/// </summary>
public static class RuntimeStatsEndpoint
{
    private static readonly System.Diagnostics.Stopwatch _uptime = System.Diagnostics.Stopwatch.StartNew();

    public static void Map(WebApplication app, string component)
    {
        app.MapGet("/stats/runtime", () =>
        {
            var gc = System.GC.GetGCMemoryInfo();
            var p = System.Diagnostics.Process.GetCurrentProcess();
            p.Refresh();
            var gens = gc.GenerationInfo;

            ThreadPool.GetAvailableThreads(out var workerAvail, out var ioAvail);
            ThreadPool.GetMaxThreads(out var workerMax, out var ioMax);
            ThreadPool.GetMinThreads(out var workerMin, out var ioMin);
            var threadpoolQueue = ThreadPool.PendingWorkItemCount;

            double uptimeSec = _uptime.Elapsed.TotalSeconds;
            double gcPauseSec = 0;
            try { gcPauseSec = System.GC.GetTotalPauseDuration().TotalSeconds; } catch { /* runtime < 7 */ }
            double timeInGcPct = uptimeSec > 0 ? gcPauseSec * 100.0 / uptimeSec : 0;

            int openFdCount = TryReadOpenFdCount();

            // RocksDB stats only meaningful on agents (the only component with a
            // RocksDB attached). The DI lookup tolerates a missing service; we
            // emit zero-valued fields rather than failing the scrape.
            object? rocksdbChunk = null;
            object? rocksdbBucket = null;
            long uniqueChunks = 0, totalChunkBytes = 0, totalBuckets = 0, totalVectors = 0;
            try
            {
                var storage = app.Services.GetService<INetworkFileStorageService>();
                if (storage is RocksDbStorageService rocks)
                {
                    var stats = rocks.GetEngineStats();
                    rocksdbChunk  = ToJson(stats.Chunk);
                    rocksdbBucket = ToJson(stats.Bucket);
                    (uniqueChunks, totalChunkBytes) = rocks.GetChunkStorageStats();
                    (totalBuckets, totalVectors) = rocks.BucketStorage.GetBucketAndVectorStats();
                }
            }
            catch { /* best-effort telemetry; never fails the scrape */ }

            return Results.Json(new
            {
                ts_unix_ms              = System.DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                component               = component,
                pod                     = System.Environment.GetEnvironmentVariable("MY_POD_NAME") ?? "unknown",
                node                    = System.Environment.GetEnvironmentVariable("MY_NODE_NAME") ?? "unknown",
                process_uptime_sec      = uptimeSec,

                heap_size_bytes         = gc.HeapSizeBytes,
                heap_committed_bytes    = gc.TotalCommittedBytes,
                heap_fragmented_bytes   = gc.FragmentedBytes,

                gen0_size               = gens.Length > 0 ? gens[0].SizeAfterBytes : 0,
                gen1_size               = gens.Length > 1 ? gens[1].SizeAfterBytes : 0,
                gen2_size               = gens.Length > 2 ? gens[2].SizeAfterBytes : 0,
                loh_size                = gens.Length > 3 ? gens[3].SizeAfterBytes : 0,
                loh_fragmented_bytes    = gens.Length > 3 ? gens[3].FragmentationAfterBytes : 0,
                poh_size                = gens.Length > 4 ? gens[4].SizeAfterBytes : 0,

                gen0_collections        = System.GC.CollectionCount(0),
                gen1_collections        = System.GC.CollectionCount(1),
                gen2_collections        = System.GC.CollectionCount(2),

                gc_pause_total_sec      = gcPauseSec,
                time_in_gc_pct          = timeInGcPct,

                rss_bytes               = p.WorkingSet64,
                private_bytes           = p.PrivateMemorySize64,
                memory_load_bytes       = gc.MemoryLoadBytes,
                memory_load_threshold   = gc.HighMemoryLoadThresholdBytes,
                native_overhead_bytes   = System.Math.Max(0, p.WorkingSet64 - gc.HeapSizeBytes),

                threadpool_workers_busy       = System.Math.Max(0, workerMax - workerAvail),
                threadpool_workers_max        = workerMax,
                threadpool_workers_min        = workerMin,
                threadpool_completion_busy    = System.Math.Max(0, ioMax - ioAvail),
                threadpool_completion_max     = ioMax,
                threadpool_completion_min     = ioMin,
                threadpool_queue_length       = threadpoolQueue,

                open_fd_count           = openFdCount,

                // ── Storage cardinality counters (in-memory, O(1) reads) ──
                rocks_unique_chunks       = uniqueChunks,
                rocks_total_chunk_bytes   = totalChunkBytes,
                rocks_total_buckets       = totalBuckets,
                rocks_total_vectors       = totalVectors,

                // ── Replication health (Phase 7) ──
                // Agent doesn't run a local consistent-hash ring (the
                // ring lives on cross/gateway and is fed by etcd), so
                // we surface the env-driven knobs that the agent's own
                // self-registration uses, plus a count of peers seen
                // recently in etcd if anti-entropy is wired in.
                replication_factor  = ParseEnvInt("AGENT_REPLICATION_FACTOR", 3),
                vnodes_per_agent    = ParseEnvInt("VNODES_PER_AGENT", 256),
                anti_entropy_enabled = string.Equals(System.Environment.GetEnvironmentVariable("ANTI_ENTROPY_ENABLED"),
                    "true", System.StringComparison.OrdinalIgnoreCase),
                anti_entropy_auto_repair = string.Equals(System.Environment.GetEnvironmentVariable("ANTI_ENTROPY_AUTO_REPAIR"),
                    "true", System.StringComparison.OrdinalIgnoreCase),

                // ── RocksDB engine stats. Two DBs (chunk + bucket) so we report
                //    both. Each is a flat object keyed by the property name
                //    matching RocksDbEnginePerDbStats. ────────────────────────
                rocksdb_chunk             = rocksdbChunk,
                rocksdb_bucket            = rocksdbBucket,
            });
        });
    }

    /// <summary>Project per-DB stats into a flat snake_case object for JSON output.</summary>
    private static object ToJson(RocksDbEnginePerDbStats s) => new
    {
        total_sst_bytes              = s.TotalSstBytes,
        live_sst_bytes               = s.LiveSstBytes,
        pending_compaction_bytes     = s.PendingCompactionBytes,
        compaction_pending           = s.CompactionPending,
        write_stopped                = s.WriteStopped,
        memtable_bytes               = s.MemtableBytes,
        immutable_memtable_count     = s.ImmutableMemtableCount,
        block_cache_used_bytes       = s.BlockCacheUsedBytes,
        block_cache_pinned_bytes     = s.BlockCachePinnedBytes,
        table_reader_mem_bytes       = s.TableReaderMemBytes,
        estimate_num_keys            = s.EstimateNumKeys,
        num_files_per_level          = s.NumFilesPerLevel,
        actual_delayed_write_rate    = s.ActualDelayedWriteRate,
    };

    private static int TryReadOpenFdCount()
    {
        try
        {
            const string path = "/proc/self/fd";
            if (!System.IO.Directory.Exists(path)) return 0;
            int count = 0;
            foreach (var _ in System.IO.Directory.EnumerateFileSystemEntries(path)) count++;
            return count;
        }
        catch { return 0; }
    }

    private static int ParseEnvInt(string name, int fallback)
    {
        var raw = System.Environment.GetEnvironmentVariable(name);
        return int.TryParse(raw, out var v) && v > 0 ? v : fallback;
    }
}
