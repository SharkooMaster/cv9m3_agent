using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text;
using Agent.Interfaces.Infs;
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
    private static readonly SemaphoreSlim _dbSemaphore = new(GetDbConcurrencyLimit(), GetDbConcurrencyLimit());
    private static readonly ConcurrentDictionary<string, long> _missingBucketCache = new();
    private static readonly ConcurrentDictionary<string, DateTime> _agentListCache = new();
    private static DateTime _agentListCacheTime = DateTime.MinValue;
    private static readonly object _agentListLock = new object();

    public RocksDbStorageService(string rocksDbPath, string postgresConnectionString)
    {
        if (string.IsNullOrWhiteSpace(rocksDbPath))
            throw new ArgumentException("RocksDB path cannot be empty.", nameof(rocksDbPath));

        Directory.CreateDirectory(rocksDbPath);
        var rocksOptions = new DbOptions().SetCreateIfMissing(true);
        _rocksDb = RocksDb.Open(rocksOptions, rocksDbPath);
        _dataSource = NpgsqlDataSource.Create(BuildConnectionString(postgresConnectionString));
        _myPodName = Environment.GetEnvironmentVariable("MY_POD_NAME") ?? Environment.GetEnvironmentVariable("MY_POD_IP") ?? "unknown";
        
        // Ensure chunk_owners table exists
        _ = Task.Run(async () => await EnsureChunkOwnersTableAsync());
        
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
        
        // Store locally in RocksDB
        _rocksDb.Put(Encoding.UTF8.GetBytes(key), data.chunk);
        
        // Record ownership in Postgres (this agent has it)
        await RecordChunkOwnershipAsync(key, _myPodName);
        
        // Replicate to N other agents (fire and forget for performance)
        _ = Task.Run(async () => await ReplicateChunkAsync(key, data.chunk));
        
        return await InsertChunkMetadataAsync(data.vector, key, data.chunk.Length, bucket_Id);
    }

    public async Task<byte[]?> GetChunkAsync(string storageGuid)
    {
        if (string.IsNullOrWhiteSpace(storageGuid))
            return null;

        var key = NormalizeStorageKey(storageGuid);
        
        // Try local RocksDB first (fast path)
        byte[]? bytes = _rocksDb.Get(Encoding.UTF8.GetBytes(key));
        if (bytes != null && bytes.Length > 0)
        {
            // Cache ownership if we have it locally
            _ = Task.Run(async () => await RecordChunkOwnershipAsync(key, _myPodName));
            return bytes;
        }
        
        // Local miss - try remote fetch
        return await FetchChunkFromRemoteAsync(key);
    }

    public async Task<byte[]?> GetChunkByReferenceAsync(ulong bucketId, ulong bucketIndex)
    {
        await _dbSemaphore.WaitAsync();
        try
        {
            await using var conn = await _dataSource.OpenConnectionAsync();
            var cmd = new NpgsqlCommand(@"
                SELECT storage_guid
                FROM vectors
                WHERE bucket_id = @bucketId AND bucket_index = @bucketIndex
                LIMIT 1;
            ", conn);
            cmd.Parameters.AddWithValue("@bucketId", (long)bucketId);
            cmd.Parameters.AddWithValue("@bucketIndex", (long)bucketIndex);

            var storageGuidObj = await cmd.ExecuteScalarAsync();
            var storageGuid = Convert.ToString(storageGuidObj);
            if (string.IsNullOrWhiteSpace(storageGuid))
                return null;
            return await GetChunkAsync(storageGuid);
        }
        finally
        {
            _dbSemaphore.Release();
        }
    }

    private static int GetDbConcurrencyLimit()
    {
        var raw = Environment.GetEnvironmentVariable("AGENT_DB_CONCURRENCY");
        if (int.TryParse(raw, out var v) && v > 0)
            return v;
        return 24;
    }

    private static int GetMissingBucketTtlSeconds()
    {
        var raw = Environment.GetEnvironmentVariable("AGENT_MISSING_BUCKET_TTL_SEC");
        if (int.TryParse(raw, out var v) && v > 0)
            return v;
        return 20;
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
        if (b.MinPoolSize < 0)
            b.MinPoolSize = 0;
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

    private async Task<(int, int)> InsertChunkMetadataAsync(float[] vector, string storagePath, int size, string bucketName)
    {
        await _dbSemaphore.WaitAsync();
        try
        {
            await using var conn = await _dataSource.OpenConnectionAsync();

            int bucketId;
            var checkBucketCmd = new NpgsqlCommand("SELECT id FROM bucket_keys WHERE bucket_name = @bucketName LIMIT 1", conn);
            checkBucketCmd.Parameters.AddWithValue("@bucketName", bucketName);
            var existingId = await checkBucketCmd.ExecuteScalarAsync();

            if (existingId != null)
            {
                bucketId = (int)existingId;
            }
            else
            {
                var insertBucketCmd = new NpgsqlCommand(@"
                    INSERT INTO bucket_keys (bucket_name, usage_count, next_index)
                    VALUES (@bucketName, 0, 1)
                    RETURNING id
                ", conn);
                insertBucketCmd.Parameters.AddWithValue("@bucketName", bucketName);
                bucketId = (int)(await insertBucketCmd.ExecuteScalarAsync())!;
            }

            var getNextIndexCmd = new NpgsqlCommand(@"
                UPDATE bucket_keys
                SET next_index = next_index + 1
                WHERE id = @bucketId
                RETURNING next_index - 1 AS bucket_index
            ", conn);
            getNextIndexCmd.Parameters.AddWithValue("@bucketId", bucketId);
            int bucketIndex = (int)(await getNextIndexCmd.ExecuteScalarAsync())!;

            var insertVectorCmd = new NpgsqlCommand(@"
                INSERT INTO vectors (vector, storage_guid, size, created_at, bucket_id, bucket_index)
                VALUES (@vector, @storagePath, @size, NOW(), @bucketId, @bucketIndex)
            ", conn);
            insertVectorCmd.Parameters.AddWithValue("@vector", vector);
            insertVectorCmd.Parameters.AddWithValue("@storagePath", storagePath);
            insertVectorCmd.Parameters.AddWithValue("@size", size);
            insertVectorCmd.Parameters.AddWithValue("@bucketId", bucketId);
            insertVectorCmd.Parameters.AddWithValue("@bucketIndex", bucketIndex);
            await insertVectorCmd.ExecuteNonQueryAsync();

            return (bucketId, bucketIndex);
        }
        finally
        {
            _dbSemaphore.Release();
        }
    }

    private async Task<List<(float[] vector, string storageGuid, long id, long index)>> GetVectorsByBucketAsync(string bucketName)
    {
        var results = new List<(float[] vector, string storageGuid, long id, long index)>();
        await _dbSemaphore.WaitAsync();
        try
        {
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
        finally
        {
            _dbSemaphore.Release();
        }
    }

    private async Task EnsureChunkOwnersTableAsync()
    {
        await _dbSemaphore.WaitAsync();
        try
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
            ", conn);
            await cmd.ExecuteNonQueryAsync();
            Console.WriteLine("[Storage] chunk_owners table verified/created");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[Storage] Failed to ensure chunk_owners table: {ex.Message}");
        }
        finally
        {
            _dbSemaphore.Release();
        }
    }

    private async Task RecordChunkOwnershipAsync(string chunkKey, string agentPodName)
    {
        await _dbSemaphore.WaitAsync();
        try
        {
            await using var conn = await _dataSource.OpenConnectionAsync();
            var cmd = new NpgsqlCommand(@"
                INSERT INTO chunk_owners (chunk_key, agent_pod_name, created_at)
                VALUES (@chunkKey, @agentPodName, NOW())
                ON CONFLICT (chunk_key, agent_pod_name) DO NOTHING;
            ", conn);
            cmd.Parameters.AddWithValue("@chunkKey", chunkKey);
            cmd.Parameters.AddWithValue("@agentPodName", agentPodName);
            await cmd.ExecuteNonQueryAsync();
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[Storage] Failed to record chunk ownership for {chunkKey} on {agentPodName}: {ex.Message}");
        }
        finally
        {
            _dbSemaphore.Release();
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
            var headlessService = Globals.AgentsLoadbalancer.Contains("headless") 
                ? Globals.AgentsLoadbalancer 
                : "agent-headless.crossv9.svc.cluster.local";
            
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
        catch (Exception ex)
        {
            Console.WriteLine($"[Storage] Failed to discover agents via DNS: {ex.Message}");
        }

        // Fallback: query Postgres for known agents
        if (agents.Count == 0)
        {
            await _dbSemaphore.WaitAsync();
            try
            {
                await using var conn = await _dataSource.OpenConnectionAsync();
                var cmd = new NpgsqlCommand(@"
                    SELECT DISTINCT agent_pod_name 
                    FROM chunk_owners 
                    WHERE agent_pod_name != @myPodName
                    ORDER BY created_at DESC
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
            catch (Exception ex)
            {
                Console.WriteLine($"[Storage] Failed to query agents from Postgres: {ex.Message}");
            }
            finally
            {
                _dbSemaphore.Release();
            }
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
        try
        {
            using var client = new TcpClient();
            var connectTask = client.ConnectAsync(ipOrHost, port);
            var timeoutTask = Task.Delay(TimeSpan.FromSeconds(2));
            var completedTask = await Task.WhenAny(connectTask, timeoutTask);
            return completedTask == connectTask && client.Connected;
        }
        catch
        {
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
        {
            Console.WriteLine($"[Storage] No other agents available for replication of {chunkKey}");
            return;
        }

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

                if (res.Success)
                {
                    Console.WriteLine($"[Storage] Replicated chunk {chunkKey} to {agentPod}");
                }
                else
                {
                    Console.WriteLine($"[Storage] Replication to {agentPod} failed: {res.ErrorMessage}");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[Storage] Failed to replicate {chunkKey} to {agentPod}: {ex.Message}");
            }
        });

        await Task.WhenAll(replicationTasks);
    }

    public async Task StoreChunkByKeyInternalAsync(string chunkKey, byte[] chunkData)
    {
        // Verify chunk key matches data
        var expectedKey = GenerateChunkKey(chunkData);
        if (expectedKey != chunkKey)
        {
            throw new ArgumentException($"Chunk key mismatch: expected {expectedKey}, got {chunkKey}");
        }

        // Store locally in RocksDB
        _rocksDb.Put(Encoding.UTF8.GetBytes(chunkKey), chunkData);
        
        // Record ownership
        await RecordChunkOwnershipAsync(chunkKey, _myPodName);
    }

    private async Task RecordRemoteChunkOwnershipAsync(string chunkKey, string agentPod)
    {
        // This method is no longer needed - we now use StoreChunkByKey RPC
        // Keeping for backwards compatibility but it's a no-op
    }

    private async Task<byte[]?> FetchChunkFromRemoteAsync(string chunkKey)
    {
        // Query Postgres for agents that have this chunk
        var ownerAgents = new List<string>();
        await _dbSemaphore.WaitAsync();
        try
        {
            await using var conn = await _dataSource.OpenConnectionAsync();
            var cmd = new NpgsqlCommand(@"
                SELECT DISTINCT agent_pod_name 
                FROM chunk_owners 
                WHERE chunk_key = @chunkKey
                ORDER BY created_at ASC;
            ", conn);
            cmd.Parameters.AddWithValue("@chunkKey", chunkKey);
            await using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                ownerAgents.Add(reader.GetString(0));
            }
        }
        finally
        {
            _dbSemaphore.Release();
        }

        if (ownerAgents.Count == 0)
        {
            Console.WriteLine($"[Storage] No owners found for chunk {chunkKey}");
            return null;
        }

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
                    await RecordChunkOwnershipAsync(chunkKey, _myPodName);
                    
                    Console.WriteLine($"[Storage] Fetched chunk {chunkKey} from remote agent {agentPod}");
                    return chunkData;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[Storage] Failed to fetch {chunkKey} from {agentPod}: {ex.Message}");
                // Continue to next agent
            }
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

