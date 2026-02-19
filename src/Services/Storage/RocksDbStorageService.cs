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
    private readonly RocksDbBucketStorage _bucketStorage; // For buckets/vectors
    private readonly RedisChunkOwnershipService _ownershipService; // For chunk_owners
    private readonly string _myPodName;
    private static readonly ConcurrentDictionary<string, long> _missingBucketCache = new();
    private static readonly ConcurrentDictionary<string, DateTime> _agentListCache = new();
    private static DateTime _agentListCacheTime = DateTime.MinValue;
    private static readonly object _agentListLock = new object();

    public RocksDbStorageService(string rocksDbPath, string postgresConnectionString)
    {
        if (string.IsNullOrWhiteSpace(rocksDbPath))
            throw new ArgumentException("RocksDB path cannot be empty.", nameof(rocksDbPath));

        // Initialize chunk storage RocksDB
        Directory.CreateDirectory(rocksDbPath);
        var rocksOptions = new DbOptions().SetCreateIfMissing(true);
        _rocksDb = RocksDb.Open(rocksOptions, rocksDbPath);
        
        // Initialize bucket/vector storage (separate RocksDB instance)
        _bucketStorage = new RocksDbBucketStorage(rocksDbPath);
        
        // Initialize Redis for chunk ownership
        var redisHost = Environment.GetEnvironmentVariable("REDIS_HOST");
        if (string.IsNullOrWhiteSpace(redisHost))
            throw new InvalidOperationException("REDIS_HOST environment variable is required");
        var redisPort = int.Parse(Environment.GetEnvironmentVariable("REDIS_PORT") ?? "6379");
        _ownershipService = new RedisChunkOwnershipService(redisHost, redisPort);
        
        // Use pod IP for chunk_owners so other agents can connect via gRPC
        _myPodName = Environment.GetEnvironmentVariable("MY_POD_IP") ?? Environment.GetEnvironmentVariable("MY_POD_NAME") ?? "unknown";
        
        Console.WriteLine($"[Storage] Using RocksDB backend path={rocksDbPath}, pod={_myPodName}");
        Console.WriteLine($"[Storage] Buckets/vectors stored in RocksDB, chunk_owners in Redis");
    }

    public void Dispose()
    {
        _rocksDb?.Dispose();
        _bucketStorage?.Dispose();
        _ownershipService?.Dispose();
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

        // Store vector in RocksDB bucket storage (handles dedup internally)
        var (bucketId, bucketIndex) = _bucketStorage.StoreVector(bucket_Id, data.vector, key, data.chunk.Length);
        
        // Store chunk locally in RocksDB
        _rocksDb.Put(Encoding.UTF8.GetBytes(key), data.chunk);

        // Cache in memory for fast search-time fetch
        ChunkCacheHandler.CacheChunk(key, data.chunk);
        
        // Record ownership in Redis (non-blocking, queued)
        _ownershipService.RecordOwnership(key);
        
        // Replicate to N other agents (fire and forget for performance)
        _ = Task.Run(async () =>
        {
            try { await ReplicateChunkAsync(key, data.chunk); }
            catch { /* best-effort background replication — swallow to prevent unobserved task exceptions */ }
        });
        
        return ((int)bucketId, (int)bucketIndex);
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
        // Look up storage GUID from RocksDB bucket storage
        var storageGuid = _bucketStorage.GetStorageGuidByReference(bucketId, bucketIndex);
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

        // Fallback: query Redis for known agents
        if (agents.Count == 0)
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
        
        // Record ownership in Redis (non-blocking, queued)
        _ownershipService.RecordOwnership(chunkKey);
        return Task.CompletedTask;
    }

    private async Task<byte[]?> FetchChunkFromRemoteAsync(string chunkKey)
    {
        // Get owner agents from Redis
        var ownerAgents = await _ownershipService.GetChunkOwnersAsync(chunkKey);
        
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
                    _ownershipService.RecordOwnership(chunkKey);
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

