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
    private readonly NpgsqlDataSource _dataSource;
    private readonly RocksDb _rocksDb;
    private readonly string _myPodName;
    private static readonly ConcurrentDictionary<string, long> _missingBucketCache = new();
    private static readonly ConcurrentDictionary<string, DateTime> _agentListCache = new();
    private static DateTime _agentListCacheTime = DateTime.MinValue;
    private static readonly object _agentListLock = new object();

    // ── Ownership write-behind batch ──
    private static readonly ConcurrentQueue<(string chunkKey, string agent)> _ownershipQueue = new();
    private static readonly Timer _ownershipFlushTimer = new(_ => FlushOwnershipQueueAsync().ConfigureAwait(false), null, 500, 500);
    private static NpgsqlDataSource? _sharedDataSource;

    // ── Ownership read cache (chunkKey → list of agent IPs) ──
    private static readonly ConcurrentDictionary<string, (List<string> agents, DateTime ts)> _ownershipCache = new();

    public RocksDbStorageService(string rocksDbPath, string postgresConnectionString)
    {
        if (string.IsNullOrWhiteSpace(rocksDbPath))
            throw new ArgumentException("RocksDB path cannot be empty.", nameof(rocksDbPath));

        Directory.CreateDirectory(rocksDbPath);
        var rocksOptions = new DbOptions().SetCreateIfMissing(true);
        _rocksDb = RocksDb.Open(rocksOptions, rocksDbPath);
        _dataSource = NpgsqlDataSource.Create(BuildConnectionString(postgresConnectionString));
        _sharedDataSource = _dataSource;
        // Use pod IP for chunk_owners so other agents can connect via gRPC (pod names aren't DNS-resolvable in a DaemonSet)
        _myPodName = Environment.GetEnvironmentVariable("MY_POD_IP") ?? Environment.GetEnvironmentVariable("MY_POD_NAME") ?? "unknown";
        
        // Ensure chunk_owners table exists (synchronous with timeout)
        try
        {
            var createTableTask = EnsureChunkOwnersTableAsync();
            if (createTableTask.Wait(TimeSpan.FromSeconds(10)))
            {
                Console.WriteLine($"[Storage] chunk_owners table verified/created successfully");
            }
            else
            {
                Console.WriteLine($"[Storage] WARNING: chunk_owners table creation timed out, will retry on first use");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[Storage] ERROR: Failed to create chunk_owners table: {ex.Message}");
            Console.WriteLine($"[Storage] Stack trace: {ex.StackTrace}");
        }
        
        Console.WriteLine($"[Storage] Using RocksDB backend path={rocksDbPath}, pod={_myPodName}");
    }

    public void Dispose()
    {
        _rocksDb?.Dispose();
        _dataSource?.Dispose();
    }

    public async Task<M_Bucket> ReadBucket(string bucket_Id)
    {
        var result = new M_Bucket(bucket_Id);
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

    public async Task<(int, int)> StoreVector(string bucket_Id, M_Data data)
    {
        if (data.chunk == null || data.chunk.Length == 0)
            throw new ArgumentException("Chunk data cannot be null or empty", nameof(data));
        if (data.vector == null || data.vector.Length == 0)
            throw new ArgumentException("Vector data cannot be null or empty", nameof(data));

        var key = GenerateChunkKey(data.chunk);

        // ── Propagate the storage key back to the M_Data so in-memory bucket entries
        //    have a valid storageGuid for future cache/disk lookups. ──
        data.storageGuid = key;

        // ── Use ONE Postgres connection for dedup check + insert ──
        // This halves connection pool pressure under high concurrency.
        await using var conn = await _dataSource.OpenConnectionAsync();

        // Dedup fast-path: if this exact chunk already has a vector entry, return it.
        var existing = await TryGetExistingVectorInternal(conn, key);
        if (existing.HasValue)
            return existing.Value;
        
        // Store locally in RocksDB
        _rocksDb.Put(Encoding.UTF8.GetBytes(key), data.chunk);

        // Cache in memory for fast search-time fetch
        ChunkCacheHandler.CacheChunk(key, data.chunk);
        
        // Batch ownership write (non-blocking)
        _ownershipQueue.Enqueue((key, _myPodName));
        
        // Replicate to N other agents (fire and forget for performance)
        _ = Task.Run(async () =>
        {
            try { await ReplicateChunkAsync(key, data.chunk); }
            catch { /* best-effort background replication — swallow to prevent unobserved task exceptions */ }
        });
        
        return await InsertChunkMetadataInternal(conn, data.vector, key, data.chunk.Length, bucket_Id);
    }

    /// <summary>
    /// Check if this chunk (by storage_guid / SHA256 key) already has a row in the vectors table.
    /// Uses a provided connection to avoid opening a second one.
    /// </summary>
    private static async Task<(int, int)?> TryGetExistingVectorInternal(NpgsqlConnection conn, string storageGuid)
    {
        var cmd = new NpgsqlCommand(@"
            SELECT bucket_id, bucket_index FROM vectors WHERE storage_guid = @sg LIMIT 1;
        ", conn);
        cmd.Parameters.AddWithValue("@sg", storageGuid);
        await using var reader = await cmd.ExecuteReaderAsync();
        if (await reader.ReadAsync())
            return (reader.GetInt32(0), reader.GetInt32(1));
        return null;
    }

    public async Task<byte[]?> GetChunkAsync(string storageGuid)
    {
        if (string.IsNullOrWhiteSpace(storageGuid))
            return null;

        var key = NormalizeStorageKey(storageGuid);
        
        // Try local RocksDB first (fast path — no Postgres hit)
        byte[]? bytes = _rocksDb.Get(Encoding.UTF8.GetBytes(key));
        if (bytes != null && bytes.Length > 0)
            return bytes;
        
        // Local miss - try remote fetch
        return await FetchChunkFromRemoteAsync(key);
    }

    public async Task<byte[]?> GetChunkByReferenceAsync(ulong bucketId, ulong bucketIndex)
    {
        // Look up the storage key first, then release the connection before calling GetChunkAsync
        string? storageGuid;
        await using (var conn = await _dataSource.OpenConnectionAsync())
        {
            var cmd = new NpgsqlCommand(@"
                SELECT storage_guid
                FROM vectors
                WHERE bucket_id = @bucketId AND bucket_index = @bucketIndex
                LIMIT 1;
            ", conn);
            cmd.Parameters.AddWithValue("@bucketId", (long)bucketId);
            cmd.Parameters.AddWithValue("@bucketIndex", (long)bucketIndex);

            var storageGuidObj = await cmd.ExecuteScalarAsync();
            storageGuid = Convert.ToString(storageGuidObj);
        }
        // Connection is released — safe to call GetChunkAsync (which may also use Postgres)
        if (string.IsNullOrWhiteSpace(storageGuid))
            return null;
        return await GetChunkAsync(storageGuid);
    }

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
        if (b.MaxPoolSize <= 0)
            b.MaxPoolSize = 120;
        // Pre-warm the pool so hot-path requests don't pay connection-open cost.
        b.MinPoolSize = Math.Max(b.MinPoolSize, 10);
        return b.ConnectionString;
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
        var results = new List<(float[] vector, string storageGuid, long id, long index)>();
        await using var conn = await _dataSource.OpenConnectionAsync();
        var cmd = new NpgsqlCommand(@"
            SELECT v.vector, v.storage_guid, v.bucket_id, v.bucket_index
            FROM vectors v
            INNER JOIN bucket_keys b ON v.bucket_id = b.id
            WHERE b.bucket_name = @bucketName;
        ", conn);
        cmd.Parameters.AddWithValue("@bucketName", bucketName);

        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            results.Add((
                reader.GetFieldValue<float[]>(0),
                reader.GetString(1),
                reader.GetInt32(2),
                reader.GetInt32(3)));
        }
        return results;
    }

    /// <summary>
    /// Batch-fetch vectors from multiple buckets in a single Postgres round-trip.
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

        var results = new List<(float[] vector, string storageGuid, long bucketId, long bucketIndex, string bucketName)>();
        // Track which buckets returned rows so we can mark the rest as missing
        var foundBuckets = new HashSet<string>();

        await using var conn = await _dataSource.OpenConnectionAsync();
        var cmd = new NpgsqlCommand(@"
            SELECT v.vector, v.storage_guid, v.bucket_id, v.bucket_index, b.bucket_name
            FROM vectors v
            INNER JOIN bucket_keys b ON v.bucket_id = b.id
            WHERE b.bucket_name = ANY(@names);
        ", conn);
        cmd.Parameters.AddWithValue("@names", toQuery.ToArray());

        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var bName = reader.GetString(4);
            foundBuckets.Add(bName);
            results.Add((
                reader.GetFieldValue<float[]>(0),
                reader.GetString(1),
                reader.GetInt32(2),
                reader.GetInt32(3),
                bName));
        }

        // Mark buckets that returned zero rows as missing (avoids future Postgres round-trips)
        foreach (var name in toQuery)
        {
            if (!foundBuckets.Contains(name))
                MarkMissingBucket(name);
            else
                ClearMissingBucket(name);
        }

        return results;
    }

    private async Task EnsureChunkOwnersTableAsync()
    {
        await using var conn = await _dataSource.OpenConnectionAsync();
        var cmd = new NpgsqlCommand(@"
            CREATE TABLE IF NOT EXISTS chunk_owners (
                chunk_key VARCHAR(64) NOT NULL,
                agent_pod_name VARCHAR(255) NOT NULL,
                created_at TIMESTAMP DEFAULT NOW(),
                PRIMARY KEY (chunk_key, agent_pod_name)
            );
            CREATE INDEX IF NOT EXISTS idx_chunk_owners_chunk_key ON chunk_owners(chunk_key);
            CREATE INDEX IF NOT EXISTS idx_chunk_owners_agent ON chunk_owners(agent_pod_name);
            -- Critical performance indexes for the vectors <-> bucket_keys JOIN
            CREATE INDEX IF NOT EXISTS idx_vectors_bucket_id ON vectors(bucket_id);
            CREATE INDEX IF NOT EXISTS idx_vectors_storage_guid ON vectors(storage_guid);
        ", conn);
        await cmd.ExecuteNonQueryAsync();
        Console.WriteLine("[Storage] ✅ Tables/indexes verified");
    }

    /// <summary>Batched flush — runs every 500 ms via timer, inserts all queued ownership rows in one round-trip.</summary>
    private static async Task FlushOwnershipQueueAsync()
    {
        if (_ownershipQueue.IsEmpty || _sharedDataSource == null)
            return;

        var batch = new List<(string key, string agent)>();
        while (batch.Count < 500 && _ownershipQueue.TryDequeue(out var item))
            batch.Add(item);

        if (batch.Count == 0)
            return;

        try
        {
            await using var conn = await _sharedDataSource.OpenConnectionAsync();
            // Build a single multi-row INSERT
            var sb = new StringBuilder("INSERT INTO chunk_owners (chunk_key, agent_pod_name, created_at) VALUES ");
            var cmd = new NpgsqlCommand();
            cmd.Connection = conn;

            for (int i = 0; i < batch.Count; i++)
            {
                if (i > 0) sb.Append(',');
                sb.Append($"(@k{i}, @a{i}, NOW())");
                cmd.Parameters.AddWithValue($"k{i}", batch[i].key);
                cmd.Parameters.AddWithValue($"a{i}", batch[i].agent);
            }

            sb.Append(" ON CONFLICT (chunk_key, agent_pod_name) DO NOTHING;");
            cmd.CommandText = sb.ToString();
            await cmd.ExecuteNonQueryAsync();
        }
        catch
        {
            // Re-enqueue so we don't lose data
            foreach (var item in batch)
                _ownershipQueue.Enqueue(item);
        }
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
        catch { /* DNS resolve failed — fall through to Postgres fallback */ }

        // Fallback: query Postgres for known agents
        if (agents.Count == 0)
        {
            try
            {
                await using var conn = await _dataSource.OpenConnectionAsync();
                var cmd = new NpgsqlCommand(@"
                    SELECT agent_pod_name 
                    FROM chunk_owners 
                    WHERE agent_pod_name != @myPodName
                    GROUP BY agent_pod_name
                    ORDER BY MAX(created_at) DESC
                    LIMIT 10;
                ", conn);
                cmd.Parameters.AddWithValue("@myPodName", _myPodName);
                await using var reader = await cmd.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    var podName = reader.GetString(0);
                    if (await IsAgentReachableAsync(podName))
                    {
                        agents.Add(podName);
                    }
                }
            }
            catch { /* Postgres agent query failed */ }
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

    private async Task ReplicateChunkAsync(string chunkKey, byte[] chunkData)
    {
        var replicationFactor = GetReplicationFactor();
        if (replicationFactor <= 1)
            return; // No replication needed

        var agents = await GetActiveAgentPodsAsync();
        if (agents.Count == 0)
            return;

        // Deterministic selection: hash chunk key to select N agents
        var selectedAgents = SelectReplicationTargets(chunkKey, agents, replicationFactor - 1); // -1 because we already stored locally

        var replicationTasks = selectedAgents.Select(async agentPod =>
        {
            try
            {
                var client = GrpcChannelFactory.GetClient(
                    target: agentPod,
                    ctor: chan => new ChunkReferenceService.ChunkReferenceServiceClient(chan),
                    roundRobin: false,
                    port: 5000
                );

                var req = new StoreChunkByKey_Req 
                { 
                    ChunkKey = chunkKey,
                    ChunkData = ByteString.CopyFrom(chunkData)
                };
                
                var res = await client.StoreChunkByKeyAsync(
                    req,
                    deadline: DateTime.UtcNow.AddSeconds(10),
                    cancellationToken: CancellationToken.None);

                // Silently ignore success/failure — replication is best-effort background work
            }
            catch { /* best-effort replication */ }
        });

        await Task.WhenAll(replicationTasks);
    }

    public Task StoreChunkByKeyInternalAsync(string chunkKey, byte[] chunkData)
    {
        // Verify chunk key matches data
        var expectedKey = GenerateChunkKey(chunkData);
        if (expectedKey != chunkKey)
        {
            throw new ArgumentException($"Chunk key mismatch: expected {expectedKey}, got {chunkKey}");
        }

        // Store locally in RocksDB
        _rocksDb.Put(Encoding.UTF8.GetBytes(chunkKey), chunkData);
        
        // Batch ownership write (non-blocking)
        _ownershipQueue.Enqueue((chunkKey, _myPodName));
        return Task.CompletedTask;
    }

    private async Task<byte[]?> FetchChunkFromRemoteAsync(string chunkKey)
    {
        // ── Check in-memory ownership cache first ──
        List<string> ownerAgents;
        if (_ownershipCache.TryGetValue(chunkKey, out var cached) && (DateTime.UtcNow - cached.ts).TotalSeconds < 60)
        {
            ownerAgents = cached.agents;
        }
        else
        {
            // Query Postgres for agents that have this chunk
            ownerAgents = new List<string>();
            try
            {
                await using var conn = await _dataSource.OpenConnectionAsync();
                var cmd = new NpgsqlCommand(@"
                    SELECT agent_pod_name 
                    FROM chunk_owners 
                    WHERE chunk_key = @chunkKey
                    GROUP BY agent_pod_name
                    ORDER BY MIN(created_at) ASC;
                ", conn);
                cmd.Parameters.AddWithValue("@chunkKey", chunkKey);
                await using var reader = await cmd.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    ownerAgents.Add(reader.GetString(0));
                }
            }
            catch { /* chunk owner query failed */ }

            // Cache even empty results briefly to avoid hammering Postgres
            _ownershipCache[chunkKey] = (ownerAgents, DateTime.UtcNow);
        }

        if (ownerAgents.Count == 0)
            return null;

        // Try fetching from each owner agent
        foreach (var agentPod in ownerAgents)
        {
            try
            {
                var client = GrpcChannelFactory.GetClient(
                    target: agentPod,
                    ctor: chan => new ChunkReferenceService.ChunkReferenceServiceClient(chan),
                    roundRobin: false,
                    port: 5000
                );

                var req = new GetChunkByKey_Req { ChunkKey = chunkKey };
                var res = await client.GetChunkByKeyAsync(
                    req,
                    deadline: DateTime.UtcNow.AddSeconds(5),
                    cancellationToken: CancellationToken.None);

                var responseChunk = res.Chunk;
                if (res.Found && responseChunk != null && responseChunk.Length > 0)
                {
                    var chunkData = responseChunk.ToByteArray();
                    
                    // Cache locally for future access
                    _rocksDb.Put(Encoding.UTF8.GetBytes(chunkKey), chunkData);
                    _ownershipQueue.Enqueue((chunkKey, _myPodName));
                    return chunkData;
                }
            }
            catch { /* try next agent */ }
        }

        return null;
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

