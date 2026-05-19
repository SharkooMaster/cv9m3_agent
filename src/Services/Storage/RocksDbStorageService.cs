using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text;
using Agent.Interfaces.Infs;
using Agent.Modules.Storage;
using Agent.Utils.Globals;
using Google.Protobuf;
using Grpc.Core;
using Npgsql;
using RocksDbSharp;

namespace Agent.Services.Storage;

public sealed class RocksDbStorageService : INetworkFileStorageService, IDisposable
{
    private readonly RocksDb _rocksDb; // For chunk storage
    private readonly RocksDbWriteBatcher _chunkWriteBatcher; // Batches chunk writes in background
    private readonly RocksDbBucketStorage _bucketStorage; // For buckets/vectors
    private readonly RedisChunkOwnershipService? _ownershipService; // For chunk_owners (null when Redis is disabled)
    private readonly string _myPodName;
    private static readonly ConcurrentDictionary<string, long> _missingBucketCache = new();
    private static readonly ConcurrentDictionary<string, DateTime> _agentListCache = new();
    private static DateTime _agentListCacheTime = DateTime.MinValue;
    private static readonly object _agentListLock = new object();

    public RocksDbStorageService(string rocksDbPath, string postgresConnectionString, long bucketBlockCacheMb = 128, long chunkBlockCacheMb = 64)
    {
        if (string.IsNullOrWhiteSpace(rocksDbPath))
            throw new ArgumentException("RocksDB path cannot be empty.", nameof(rocksDbPath));

        // ── Chunk storage RocksDB: tuned for point lookups (Get by storageGuid) ──
        Directory.CreateDirectory(rocksDbPath);
        var chunkTableOpts = new BlockBasedTableOptions()
            .SetFilterPolicy(BloomFilterPolicy.Create(10, false))                       // Bloom filter: skip SSTs without this key
            .SetBlockCache(RocksDbSharp.Cache.CreateLru((ulong)(chunkBlockCacheMb * 1024 * 1024)))   // LRU block cache
            .SetBlockSize(8 * 1024)                                                     // 8KB blocks (chunks are ~1KB)
            .SetCacheIndexAndFilterBlocks(true)
            .SetPinL0FilterAndIndexBlocksInCache(true)
            .SetWholeKeyFiltering(true)
            .SetFormatVersion(4);

        var chunkCfOpts = new ColumnFamilyOptions()
            .SetBlockBasedTableFactory(chunkTableOpts)
            .SetWriteBufferSize(64 * 1024 * 1024)                          // 64MB memtable
            .SetMaxWriteBufferNumber(4)                                     // 4 memtables before stall
            .SetLevel0FileNumCompactionTrigger(4)                           // Compact after 4 L0 files
            .SetLevel0SlowdownWritesTrigger(20)                             // Soft throttle at 20
            .SetLevel0StopWritesTrigger(48)                                 // Hard stall at 48 (vs default 36)
            .SetMaxBytesForLevelBase(512 * 1024 * 1024)                    // 512MB L1 — reduces write amp for large datasets
            .SetCompression(Compression.Zstd);

        int bgThreads = Math.Max(4, Environment.ProcessorCount / 2);
        var rocksOptions = new DbOptions()
            .SetCreateIfMissing(true)
            .IncreaseParallelism(bgThreads)
            .SetMaxBackgroundCompactions(bgThreads)
            .SetMaxBackgroundFlushes(Math.Max(2, bgThreads / 2));
        var chunkCfs = new ColumnFamilies { { "default", chunkCfOpts } };
        _rocksDb = RocksDb.Open(rocksOptions, rocksDbPath, chunkCfs);

        // Initialize write batcher for chunks (batches writes in background, non-blocking)
        // High-throughput: Larger batches, more frequent flushes
        _chunkWriteBatcher = new RocksDbWriteBatcher(_rocksDb, batchSize: 5_000, flushIntervalMs: 5_000);

        // Initialize bucket/vector storage (separate RocksDB instance, with its own bloom+cache)
        _bucketStorage = new RocksDbBucketStorage(rocksDbPath, blockCacheSizeMb: bucketBlockCacheMb);

        Console.WriteLine($"[Storage] RocksDB chunk block cache: {chunkBlockCacheMb}MB, bucket block cache: {bucketBlockCacheMb}MB");
        
        // Initialize Redis for chunk ownership (optional — skip if REDIS_HOST is not set)
        var redisHost = Environment.GetEnvironmentVariable("REDIS_HOST");
        if (!string.IsNullOrWhiteSpace(redisHost))
        {
            var redisPort = int.Parse(Environment.GetEnvironmentVariable("REDIS_PORT") ?? "6379");
            _ownershipService = new RedisChunkOwnershipService(redisHost, redisPort);
            Console.WriteLine($"[Storage] Redis chunk ownership enabled at {redisHost}:{redisPort}");
        }
        else
        {
            _ownershipService = null;
            Console.WriteLine("[Storage] Redis disabled — chunk ownership via DNS/rendezvous only");
        }
        
        _myPodName = Environment.GetEnvironmentVariable("MY_POD_IP") ?? Environment.GetEnvironmentVariable("MY_POD_NAME") ?? "unknown";
        Console.WriteLine($"[Storage] Using RocksDB backend path={rocksDbPath}, pod={_myPodName}");
    }

    /// <summary>
    /// Flush all pending writes (chunks + bucket metadata) to RocksDB WAL.
    /// After this returns, data survives process crashes (SIGKILL, OOMKill).
    /// Called by StoreVectorService after each BatchStore/Store RPC completes.
    /// </summary>
    public void FlushPendingWrites()
    {
        _chunkWriteBatcher?.Flush();
        _bucketStorage?.FlushWrites();
    }

    public void Dispose()
    {
        _chunkWriteBatcher?.Flush(); // Flush pending writes before shutdown
        PersistStatsCounters(); // Save stats so next startup is O(1)
        _chunkWriteBatcher?.Dispose();
        _rocksDb?.Dispose();
        _bucketStorage?.Dispose();
        _ownershipService?.Dispose();
    }

    /// <summary>
    /// Pre-load buckets/vectors from RocksDB into Globals._NODE.Buckets, up to the RAM budget.
    /// Buckets that don't fit are left on disk (L2) and loaded on-demand during search/store.
    /// </summary>
    public void WarmUpBuckets(long maxCacheBytes)
    {
        var sw = System.Diagnostics.Stopwatch.StartNew();
        var allBuckets = _bucketStorage.LoadAllBucketsToMemory();
        int totalVectors = 0;
        long estimatedBytes = 0;
        int loadedBuckets = 0;
        int skippedBuckets = 0;

        // Sort by vector count descending — largest buckets are most likely to be hit
        var sorted = allBuckets.OrderByDescending(kv => kv.Value.Count);

        foreach (var (bucketName, vectors) in sorted)
        {
            long bucketBytes = vectors.Count * 470L;
            if (maxCacheBytes > 0 && estimatedBytes + bucketBytes > maxCacheBytes && estimatedBytes > 0)
            {
                skippedBuckets++;
                continue;
            }

            ulong bucketKey = RocksDbBucketStorage.BitstringToUlong(bucketName);
            var bucket = Globals._NODE.Buckets.GetOrAdd(bucketKey, _ => new M_Bucket(bucketKey));
            foreach (var (vector, storageGuid, bucketId, bucketIndex, normSquared) in vectors)
            {
                bucket.data.Add(new M_Data
                {
                    vector = vector,
                    storageGuid = storageGuid,
                    id = bucketId,
                    index = bucketIndex,
                    normSquared = normSquared,
                    chunk = null
                });
            }
            bucket.TouchAccess();
            totalVectors += vectors.Count;
            estimatedBytes += bucketBytes;
            loadedBuckets++;
        }

        sw.Stop();
        Console.WriteLine(
            $"[Storage] ✅ Warmed up {loadedBuckets} buckets ({skippedBuckets} cold on disk), " +
            $"{totalVectors} vectors into RAM (~{estimatedBytes / (1024 * 1024)}MB / {maxCacheBytes / (1024 * 1024)}MB cap) " +
            $"in {sw.ElapsedMilliseconds}ms. Total on disk: {allBuckets.Count} buckets.");
    }

    /// <summary>
    /// Expose the internal bucket storage for BucketCacheManager.
    /// </summary>
    public RocksDbBucketStorage BucketStorage => _bucketStorage;

    // ── Live storage stats — O(1) reads, no scanning ──
    // Counters track unique chunks and bytes. Initialized from a one-time startup scan
    // (or persisted values), then maintained incrementally on each Store.
    private long _liveUniqueChunks;
    private long _liveChunkBytes;
    private volatile bool _statsReady;

    // Dedup guard for stats: tracks storageGuids we've already counted.
    // Prevents double-counting when the same chunk content is stored via different buckets.
    // Uses a 32-byte value-type key (4 ulongs from SHA256 hex) instead of a ~152-byte
    // heap string to save ~140 bytes per entry (hundreds of MB at scale).
    private readonly ConcurrentDictionary<GuidKey, int> _knownChunkSizes = new();

    private readonly record struct GuidKey(ulong G0, ulong G1, ulong G2, ulong G3);

    private static GuidKey MakeGuidKey(ReadOnlySpan<char> hexGuid)
    {
        if (hexGuid.Length != 64) return default;
        Span<byte> bytes = stackalloc byte[32];
        for (int i = 0; i < 32; i++)
            bytes[i] = (byte)((HexVal(hexGuid[i * 2]) << 4) | HexVal(hexGuid[i * 2 + 1]));
        return new GuidKey(
            BinaryPrimitives.ReadUInt64LittleEndian(bytes),
            BinaryPrimitives.ReadUInt64LittleEndian(bytes.Slice(8)),
            BinaryPrimitives.ReadUInt64LittleEndian(bytes.Slice(16)),
            BinaryPrimitives.ReadUInt64LittleEndian(bytes.Slice(24)));
    }

    private static int HexVal(char c) =>
        c >= '0' && c <= '9' ? c - '0' :
        c >= 'a' && c <= 'f' ? c - 'a' + 10 :
        c >= 'A' && c <= 'F' ? c - 'A' + 10 : 0;

    // Persisted counter keys in RocksDB (prefixed to avoid collision with real chunk keys)
    private static readonly byte[] StatsChunksKey = Encoding.UTF8.GetBytes("__crossv9_stats_unique_chunks__");
    private static readonly byte[] StatsBytesKey = Encoding.UTF8.GetBytes("__crossv9_stats_total_bytes__");

    /// <summary>
    /// Initialize stats counters. Call once during startup.
    /// Tries to read persisted counters first (O(1)). If not found, does a one-time full scan.
    /// Also populates _knownChunkSizes for accurate incremental tracking.
    /// </summary>
    public void InitializeStats()
    {
        var sw = System.Diagnostics.Stopwatch.StartNew();

        // Try persisted counters first — O(1)
        var chunksBytes = _rocksDb.Get(StatsChunksKey);
        var bytesBytes = _rocksDb.Get(StatsBytesKey);

        if (chunksBytes != null && bytesBytes != null && chunksBytes.Length == 8 && bytesBytes.Length == 8)
        {
            _liveUniqueChunks = BitConverter.ToInt64(chunksBytes);
            _liveChunkBytes = BitConverter.ToInt64(bytesBytes);

            // Populate known guids from a quick key-only scan (no value reads = fast)
            long scannedKeys = 0;
            using var iterator = _rocksDb.NewIterator();
            iterator.SeekToFirst();
            while (iterator.Valid())
            {
                var keySpan = iterator.Key();
                if (keySpan != null && keySpan.Length == 64) // SHA256 hex = 64 chars
                {
                    var keyStr = Encoding.UTF8.GetString(keySpan);
                    if (!keyStr.StartsWith("__"))
                        _knownChunkSizes.TryAdd(MakeGuidKey(keyStr), 0);
                    scannedKeys++;
                }
                iterator.Next();
            }

            sw.Stop();
            Console.WriteLine($"[StorageStats] Fast init from persisted counters: {_liveUniqueChunks:N0} chunks, " +
                $"{_liveChunkBytes / (1024.0 * 1024.0):F1} MB, scanned {scannedKeys:N0} keys in {sw.ElapsedMilliseconds}ms");
        }
        else
        {
            // No persisted counters — full scan (first-ever startup)
            Console.WriteLine("[StorageStats] No persisted counters found, performing full scan...");
            long chunks = 0;
            long bytes = 0;

            using var iterator = _rocksDb.NewIterator();
            iterator.SeekToFirst();
            while (iterator.Valid())
            {
                var keySpan = iterator.Key();
                var valSpan = iterator.Value();
                var keyStr = keySpan != null ? Encoding.UTF8.GetString(keySpan) : "";

                // Skip our internal stat keys
                if (!keyStr.StartsWith("__") && keySpan != null && keySpan.Length == 64)
                {
                    chunks++;
                    int valLen = valSpan?.Length ?? 0;
                    bytes += valLen;
                    _knownChunkSizes.TryAdd(MakeGuidKey(keyStr), valLen);
                }
                iterator.Next();
            }

            _liveUniqueChunks = chunks;
            _liveChunkBytes = bytes;

            // Persist for next startup
            PersistStatsCounters();

            sw.Stop();
            Console.WriteLine($"[StorageStats] Full scan complete: {chunks:N0} unique chunks, " +
                $"{bytes / (1024.0 * 1024.0):F1} MB in {sw.ElapsedMilliseconds}ms (persisted for next startup)");
        }

        _statsReady = true;
    }

    /// <summary>
    /// Persist current counters to RocksDB so next startup is O(1).
    /// Called periodically and on shutdown.
    /// </summary>
    public void PersistStatsCounters()
    {
        _rocksDb.Put(StatsChunksKey, BitConverter.GetBytes(Interlocked.Read(ref _liveUniqueChunks)));
        _rocksDb.Put(StatsBytesKey, BitConverter.GetBytes(Interlocked.Read(ref _liveChunkBytes)));
    }

    /// <summary>
    /// Track a newly stored chunk for stats. Returns true if this is a genuinely new chunk.
    /// Thread-safe. Uses _knownChunkSizes to prevent double-counting across buckets.
    /// </summary>
    internal bool TrackChunkForStats(string storageGuid, int chunkSizeBytes)
    {
        var gk = MakeGuidKey(storageGuid);
        if (_knownChunkSizes.TryAdd(gk, chunkSizeBytes))
        {
            Interlocked.Increment(ref _liveUniqueChunks);
            Interlocked.Add(ref _liveChunkBytes, chunkSizeBytes);
            return true;
        }
        return false;
    }

    /// <summary>
    /// Get total unique chunks and total bytes stored. O(1) — returns live counters.
    /// No scanning, no blocking. Safe to call at any frequency.
    /// Note: counts only THIS agent's unique chunks. No replication exists —
    /// each chunk routes to exactly one agent via Rendezvous Hashing.
    /// Summing across agents gives the true global unique total.
    /// </summary>
    public (long uniqueChunks, long totalBytes) GetChunkStorageStats()
    {
        if (!_statsReady)
            return (0, 0); // Background init not done yet

        return (Interlocked.Read(ref _liveUniqueChunks), Interlocked.Read(ref _liveChunkBytes));
    }

    public async Task<M_Bucket> ReadBucket(string bucket_Id)
    {
        var result = new M_Bucket(RocksDbBucketStorage.BitstringToUlong(bucket_Id));
        if (IsKnownMissingBucket(bucket_Id))
            return result;

        var rows = await GetVectorsByBucketAsync(bucket_Id);
        if (rows.Count == 0)
        {
            MarkMissingBucket(bucket_Id);
            return result;
        }
        ClearMissingBucket(bucket_Id);

        foreach (var (vector, storageGuid, id, index) in rows)
        {
            result.data.Add(new M_Data
            {
                vector = vector,
                chunk = null,
                storageGuid = storageGuid,
                id = (ulong)id,
                index = (ulong)index
            });
        }
        return result;
    }

    public async Task<(ulong, ulong)> StoreVector(string bucket_Id, M_Data data)
    {
        if (data.chunk == null || data.chunk.Length == 0)
            throw new ArgumentException("Chunk data cannot be null or empty", nameof(data));
        if (data.vector == null || data.vector.Length == 0)
            throw new ArgumentException("Vector data cannot be null or empty", nameof(data));

        var key = GenerateChunkKey(data.chunk);

        // ── Propagate the storage key back to the M_Data so in-memory bucket entries
        //    have a valid storageGuid for future cache/disk lookups. ──
        data.storageGuid = key;

        int chunkLen = data.chunk.Length;

        // Store vector in RocksDB bucket storage (handles dedup internally)
        var (bucketId, bucketIndex) = _bucketStorage.StoreVector(bucket_Id, data.vector, key, chunkLen);
        
        // Store chunk locally in RocksDB (batched, non-blocking background write)
        _chunkWriteBatcher.Put(key, data.chunk);

        // Track for live stats — only counts genuinely new chunks (dedup-safe across buckets)
        TrackChunkForStats(key, chunkLen);

        // Cache in memory for fast search-time fetch
        ChunkCacheHandler.CacheChunk(key, data.chunk);
        
        return (bucketId, bucketIndex);
    }


    public async Task<byte[]?> GetChunkAsync(string storageGuid)
    {
        if (string.IsNullOrWhiteSpace(storageGuid))
            return null;

        var key = NormalizeStorageKey(storageGuid);

        // ── CRITICAL: Check MRU cache FIRST ──
        // Chunks are CacheChunk'd immediately in StoreVector, but the RocksDB write
        // batcher may not have flushed yet (up to 50ms / 500 items delay).
        // Without this check, any GetChunkByReferenceAsync called before the flush
        // would miss the chunk → return null → Cross diffs against zeros → file bloat + corruption.
        var cached = ChunkCacheHandler.GetFromCacheOnly(key);
        if (cached != null && cached.Length > 0)
            return cached;

        // Try local RocksDB (chunk may have been flushed by now)
        byte[]? bytes = _rocksDb.Get(Encoding.UTF8.GetBytes(key));
        if (bytes != null && bytes.Length > 0)
            return bytes;
        
        // Local miss - try remote fetch
        return await FetchChunkFromRemoteAsync(key);
    }

    public async Task<byte[]?> GetChunkByReferenceAsync(ulong bucketId, ulong bucketIndex)
    {
        // 1. Try in-memory bucket FIRST, regardless of L1Enabled.
        //
        // Rationale: SearchVector reads from Globals._NODE.Buckets (in-memory)
        // and hands cross a (BucketId, BucketKey, StorageGuid) triple. Cross
        // encodes the diff against that exact StorageGuid. If decompress (this
        // method) skips the in-memory snapshot and goes straight to RocksDB,
        // a vector record that was added in-memory but hasn't been flushed by
        // the write batcher yet — or that maps to a different storageGuid in
        // RocksDB because of a stale `bnext:` counter — will return wrong
        // bytes. The diff was computed against StorageGuid_X but decompress
        // resolves the same (B, K) to StorageGuid_Y, and patches go on the
        // wrong base. The whole-block SHA fails and the operator sees
        // "block N/M: exp=... got=...".
        //
        // The in-memory bucket is always the source of truth for what search
        // returned, so always consult it first. The L1Enabled flag still
        // controls *promotion* into the L1 cache; it must not gate reads of
        // entries that are already there.
        if (Globals._NODE.Buckets.TryGetValue(bucketId, out var bucket))
        {
            var snapshot = bucket.GetDataSnapshot();
            for (int j = 0; j < snapshot.Length; j++)
            {
                if (snapshot[j] != null && snapshot[j].index == bucketIndex
                    && !string.IsNullOrEmpty(snapshot[j].storageGuid))
                {
                    var chunk = await GetChunkAsync(snapshot[j].storageGuid);
                    if (chunk != null) return chunk;
                    break;
                }
            }
        }

        // 2. RocksDB bucket metadata → storageGuid (O(1) point lookup)
        var storageGuid = _bucketStorage.GetStorageGuidByReference(bucketId, bucketIndex);
        if (string.IsNullOrWhiteSpace(storageGuid))
            return null;

        // 3. Fetch chunk bytes: MRU cache first, then local RocksDB
        return await GetChunkAsync(storageGuid);
    }

    // SearchBucketReferenceAcrossAgentsAsync REMOVED.
    // It caused an infinite recursion: Agent A → GetChunkByReference on Agent B → Agent B → GetChunkByReference on Agent A → ...
    // The fix: GetChunkByReferenceAsync uses only local RocksDB + Redis (both non-recursive).
    // If the caller needs to try multiple agents, it should do so itself (e.g., Cross retries with different agents).

    private static int GetMissingBucketTtlSeconds()
    {
        var raw = Environment.GetEnvironmentVariable("AGENT_MISSING_BUCKET_TTL_SEC");
        if (int.TryParse(raw, out var v) && v > 0)
            return v;
        return 300;   // 5 minutes — empty buckets don't spontaneously fill
    }

    private static bool IsKnownMissingBucket(string bucketId)
    {
        if (!_missingBucketCache.TryGetValue(bucketId, out var ticks))
            return false;
        var age = DateTime.UtcNow - new DateTime(ticks, DateTimeKind.Utc);
        return age.TotalSeconds <= GetMissingBucketTtlSeconds();
    }

    private static void MarkMissingBucket(string bucketId)
        => _missingBucketCache[bucketId] = DateTime.UtcNow.Ticks;

    private static void ClearMissingBucket(string bucketId)
        => _missingBucketCache.TryRemove(bucketId, out _);

    private static string BuildConnectionString(string raw)
    {
        var b = new NpgsqlConnectionStringBuilder(raw);
        // With async/await and proper pooling, connections are returned immediately after each operation.
        // With GATEWAY_STREAM_CONCURRENCY=32 and 5 agents, 25 per agent = 125 total, well under 600 limit.
        // This leaves plenty of headroom for bursts and other services.
        if (b.MaxPoolSize <= 0)
            b.MaxPoolSize = 25;
        // Pre-warm the pool so hot-path requests don't pay connection-open cost.
        b.MinPoolSize = Math.Max(b.MinPoolSize, 5);
        return b.ConnectionString;
    }

    /// <summary>
    /// Opens a Postgres connection with retry logic for "too many clients" errors.
    /// Uses exponential backoff: 50ms, 100ms, 200ms, 400ms (max 4 retries).
    /// </summary>
    private static async Task<NpgsqlConnection> OpenConnectionWithRetryAsync(NpgsqlDataSource dataSource, int maxRetries = 4)
    {
        int attempt = 0;
        while (true)
        {
            try
            {
                return await dataSource.OpenConnectionAsync();
            }
            catch (PostgresException pgEx) when (pgEx.SqlState == "53300" && attempt < maxRetries)
            {
                // "too many clients already" - retry with exponential backoff
                attempt++;
                var delayMs = Math.Min(50 * (1 << (attempt - 1)), 400); // 50, 100, 200, 400ms
                await Task.Delay(delayMs);
                // Continue loop to retry
            }
            catch (PostgresException pgEx) when (pgEx.SqlState == "53300")
            {
                // Max retries exhausted - throw with context
                throw new InvalidOperationException($"Failed to open Postgres connection after {maxRetries} retries: too many clients", pgEx);
            }
        }
    }

    private static string GenerateChunkKey(byte[] chunkData)
    {
        using var sha256 = SHA256.Create();
        var hashBytes = sha256.ComputeHash(chunkData);
        var sb = new StringBuilder(hashBytes.Length * 2);
        foreach (var b in hashBytes)
            sb.Append(b.ToString("x2"));
        return sb.ToString();
    }

    private static string NormalizeStorageKey(string storageGuid)
    {
        var key = storageGuid.Trim();
        if (key.StartsWith("rocksdb:", StringComparison.OrdinalIgnoreCase))
            key = key["rocksdb:".Length..];
        if (key.Contains('/'))
            key = key[(key.LastIndexOf('/') + 1)..];
        return key;
    }

    /// <summary>
    /// Two-step upsert inside one transaction using a provided connection.
    ///   1. UPSERT bucket_keys — atomically allocates the next index.
    ///   2. INSERT the vector row with the allocated (bucket_id, bucket_index).
    /// </summary>
    private static async Task<(int, int)> InsertChunkMetadataInternal(NpgsqlConnection conn, float[] vector, string storagePath, int size, string bucketName)
    {
        await using var tx = await conn.BeginTransactionAsync();

        try
        {
            var bucketCmd = new NpgsqlCommand(@"
                INSERT INTO bucket_keys (bucket_name, usage_count, next_index)
                VALUES (@bucketName, 1, 2)
                ON CONFLICT (bucket_name) DO UPDATE SET
                    usage_count = bucket_keys.usage_count + 1,
                    next_index  = bucket_keys.next_index  + 1
                RETURNING id, next_index - 1 AS bucket_index;
            ", conn, tx);
            bucketCmd.Parameters.AddWithValue("@bucketName", bucketName);

            int bucketId;
            int bucketIndex;
            await using (var reader = await bucketCmd.ExecuteReaderAsync())
            {
                if (!await reader.ReadAsync())
                    throw new InvalidOperationException($"InsertChunkMetadata: UPSERT returned no rows for bucket '{bucketName}'");
                bucketId    = reader.GetInt32(0);
                bucketIndex = reader.GetInt32(1);
            }

            var vectorCmd = new NpgsqlCommand(@"
                INSERT INTO vectors (vector, storage_guid, size, created_at, bucket_id, bucket_index)
                VALUES (@vector, @storagePath, @size, NOW(), @bucketId, @bucketIndex);
            ", conn, tx);
            vectorCmd.Parameters.AddWithValue("@vector", vector);
            vectorCmd.Parameters.AddWithValue("@storagePath", storagePath);
            vectorCmd.Parameters.AddWithValue("@size", size);
            vectorCmd.Parameters.AddWithValue("@bucketId", bucketId);
            vectorCmd.Parameters.AddWithValue("@bucketIndex", bucketIndex);

            await vectorCmd.ExecuteNonQueryAsync();
            await tx.CommitAsync();

            return (bucketId, bucketIndex);
        }
        catch
        {
            await tx.RollbackAsync();
            throw;
        }
    }

    private async Task<List<(float[] vector, string storageGuid, long id, long index)>> GetVectorsByBucketAsync(string bucketName)
    {
        // Read from RocksDB bucket storage
        var rocksResults = _bucketStorage.GetVectorsByBuckets(new List<string> { bucketName });
        return rocksResults.Select(r => (r.vector, r.storageGuid, (long)r.bucketId, (long)r.bucketIndex)).ToList();
    }

    /// <summary>
    /// Batch-fetch vectors from multiple buckets from RocksDB.
    /// Filters out known-missing buckets beforehand and marks newly-empty ones.
    /// Returns all vectors grouped by bucket name.
    /// </summary>
    public async Task<List<(float[] vector, string storageGuid, long bucketId, long bucketIndex, string bucketName)>> GetVectorsByBucketsAsync(List<string> bucketNames)
    {
        if (bucketNames == null || bucketNames.Count == 0)
            return new List<(float[], string, long, long, string)>();

        // Filter out buckets we already know are empty (TTL-based cache)
        var toQuery = new List<string>();
        foreach (var name in bucketNames)
        {
            if (!IsKnownMissingBucket(name))
                toQuery.Add(name);
        }

        if (toQuery.Count == 0)
            return new List<(float[], string, long, long, string)>();

        // Read from RocksDB bucket storage
        var rocksResults = _bucketStorage.GetVectorsByBuckets(toQuery);
        var results = rocksResults.Select(r => (r.vector, r.storageGuid, (long)r.bucketId, (long)r.bucketIndex, r.bucketName)).ToList();
        var foundBuckets = new HashSet<string>(results.Select(r => r.bucketName));

        // Mark buckets that returned zero rows as missing (avoids future RocksDB lookups)
        foreach (var name in toQuery)
        {
            if (!foundBuckets.Contains(name))
                MarkMissingBucket(name);
            else
                ClearMissingBucket(name);
        }

        return results;
    }


    private async Task<List<string>> GetActiveAgentPodsAsync()
    {
        lock (_agentListLock)
        {
            var now = DateTime.UtcNow;
            if (_agentListCacheTime > now.AddSeconds(-30)) // Cache for 30 seconds
            {
                return _agentListCache.Keys.ToList();
            }
        }

        var agents = new List<string>();
        try
        {
            // Try Kubernetes headless service DNS
            var headlessService = Environment.GetEnvironmentVariable("AGENT_HEADLESS_SVC")
                ?? (Globals.AgentsLoadbalancer.Contains("headless") 
                    ? Globals.AgentsLoadbalancer 
                    : "agent-headless.crossv9.svc.cluster.local");
            
            var addresses = await Dns.GetHostAddressesAsync(headlessService);
            var myIp = Environment.GetEnvironmentVariable("MY_POD_IP");
            
            foreach (var addr in addresses)
            {
                var ip = addr.ToString();
                if (ip != myIp && await IsAgentReachableAsync(ip))
                {
                    agents.Add(ip);
                }
            }
        }
        catch { /* DNS resolve failed — fall through to Redis fallback */ }

        // Fallback: query Redis for known agents (skipped when Redis is disabled)
        if (agents.Count == 0 && _ownershipService != null)
        {
            try
            {
                var redisAgents = await _ownershipService.GetActiveAgentPodsAsync();
                foreach (var podName in redisAgents)
                {
                    if (podName != _myPodName && await IsAgentReachableAsync(podName))
                    {
                        agents.Add(podName);
                    }
                }
            }
            catch { /* Redis agent query failed */ }
        }

        // Update cache
        lock (_agentListLock)
        {
            _agentListCache.Clear();
            foreach (var agent in agents)
            {
                _agentListCache[agent] = DateTime.UtcNow;
            }
            _agentListCacheTime = DateTime.UtcNow;
        }

        return agents;
    }

    private static async Task<bool> IsAgentReachableAsync(string ipOrHost, int port = 5000)
    {
        // Use Task.Run to isolate DNS/connection failures and ensure all exceptions are observed
        var reachableTask = Task.Run(async () =>
        {
            try
            {
                using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
                using var client = new TcpClient();
                await client.ConnectAsync(ipOrHost, port, cts.Token).ConfigureAwait(false);
                return client.Connected;
            }
            catch (OperationCanceledException)
            {
                return false; // Timeout
            }
            catch (SocketException)
            {
                return false; // DNS failure, connection refused, etc.
            }
            catch
            {
                return false; // Any other error
            }
        });

        try
        {
            return await reachableTask.ConfigureAwait(false);
        }
        catch
        {
            // Final safety net - ensure any unobserved exceptions are caught
            return false;
        }
    }

    private Task ReplicateChunkAsync(string chunkKey, byte[] chunkData)
    {
        // REPLICATION DISABLED — single-copy storage only.
        //
        // RISK: If this agent's disk/pod is lost, all chunks it owns are permanently gone.
        //       Compressed files referencing those chunks become undecompressible.
        //
        // MITIGATIONS (pick one or more):
        //   1. Use PersistentVolumeClaims with a replicated StorageClass (e.g., Ceph RBD, Longhorn)
        //   2. Re-enable async replication to N-1 agents (fire-and-forget background write)
        //   3. External backup of the RocksDB data directory on a schedule
        //
        // Re-enabling replication requires normalizing GetChunkByReference proto/codegen
        // across environments and deciding on replication factor + consistency model.
        return Task.CompletedTask;
    }

    public Task StoreChunkByKeyInternalAsync(string chunkKey, byte[] chunkData)
    {
        // Verify chunk key matches data
        var expectedKey = GenerateChunkKey(chunkData);
        if (expectedKey != chunkKey)
        {
            throw new ArgumentException($"Chunk key mismatch: expected {expectedKey}, got {chunkKey}");
        }

        // Store locally in RocksDB (batched, non-blocking background write)
        _chunkWriteBatcher.Put(chunkKey, chunkData);

        // Track for live stats — only counts genuinely new chunks
        TrackChunkForStats(chunkKey, chunkData.Length);

        return Task.CompletedTask;
    }

    private Task<byte[]?> FetchChunkFromRemoteAsync(string chunkKey)
    {
        // With rendezvous hashing, Cross routes to the correct agent directly.
        // If the chunk isn't in local RocksDB or MRU cache, it's not here.
        // Cross handles retry on fallback agents.
        return Task.FromResult<byte[]?>(null);
    }

    private static List<string> SelectReplicationTargets(string chunkKey, List<string> agents, int count)
    {
        if (agents.Count == 0 || count <= 0)
            return new List<string>();

        // Deterministic selection using hash of chunk key
        var hash = chunkKey.GetHashCode();
        var selected = new List<string>();
        var used = new HashSet<int>();

        for (int i = 0; i < count && selected.Count < agents.Count; i++)
        {
            var idx = Math.Abs((hash + i * 7919) % agents.Count); // 7919 is a prime for better distribution
            if (!used.Contains(idx))
            {
                selected.Add(agents[idx]);
                used.Add(idx);
            }
        }

        return selected;
    }

    private static int GetReplicationFactor()
    {
        var raw = Environment.GetEnvironmentVariable("AGENT_REPLICATION_FACTOR");
        if (int.TryParse(raw, out var v) && v > 0)
            return v;
        return 2; // Default: replicate to 2 agents (3 total including self)
    }
}

