using System.Collections.Concurrent;
using System.Text;
using System.Text.Json;
using StackExchange.Redis;

namespace Agent.Services.Storage;

/// <summary>
/// Manages chunk ownership in Redis instead of Postgres.
/// Uses Redis Sets to store which agents have each chunk.
/// </summary>
public sealed class RedisChunkOwnershipService : IDisposable
{
    private readonly IDatabase _redis;
    private readonly ConnectionMultiplexer _redisConnection;
    private readonly string _myPodName;
    private static readonly ConcurrentQueue<(string chunkKey, string agent)> _ownershipQueue = new();
    private static readonly ConcurrentQueue<(ulong bucketId, ulong bucketIndex, string storageGuid)> _bucketRefQueue = new();
    private static readonly Timer _ownershipFlushTimer = new(_ => FlushOwnershipQueueAsync().ConfigureAwait(false), null, 500, 500);
    private static readonly Timer _bucketRefFlushTimer = new(_ => FlushBucketRefQueueAsync().ConfigureAwait(false), null, 500, 500);
    private static IDatabase? _sharedRedis;

    // ── Ownership read cache (chunkKey → list of agent IPs) ──
    private static readonly ConcurrentDictionary<string, (List<string> agents, DateTime ts)> _ownershipCache = new();

    public RedisChunkOwnershipService(string redisHost, int redisPort)
    {
        var config = ConfigurationOptions.Parse($"{redisHost}:{redisPort}");
        config.AbortOnConnectFail = false;
        config.ConnectRetry = 3;
        config.ConnectTimeout = 10000; // Increased timeout
        config.SyncTimeout = 30000; // Increased sync timeout for high load (30s)
        config.AsyncTimeout = 30000; // Async timeout
        config.ResponseTimeout = 30000; // Response timeout
        
        _redisConnection = ConnectionMultiplexer.Connect(config);
        _redis = _redisConnection.GetDatabase();
        _sharedRedis = _redis;
        _myPodName = Environment.GetEnvironmentVariable("MY_POD_IP") ?? Environment.GetEnvironmentVariable("MY_POD_NAME") ?? "unknown";
        
        Console.WriteLine($"[Redis] Connected to {redisHost}:{redisPort}, pod={_myPodName}");
    }

    public void Dispose()
    {
        // Flush pending operations before shutdown
        _ = Task.Run(async () =>
        {
            await FlushOwnershipQueueAsync();
            await FlushBucketRefQueueAsync();
        });
        
        // Wait a bit for flush to complete
        Thread.Sleep(100);
        
        _ownershipFlushTimer?.Dispose();
        _bucketRefFlushTimer?.Dispose();
        _redisConnection?.Dispose();
    }

    /// <summary>
    /// Record that this agent owns a chunk (non-blocking, queued).
    /// </summary>
    public void RecordOwnership(string chunkKey)
    {
        _ownershipQueue.Enqueue((chunkKey, _myPodName));
    }

    /// <summary>
    /// Store bucket reference mapping: bucketId:bucketIndex → storageGuid.
    /// This allows any agent to look up which chunk a reference points to.
    /// Non-blocking, queued for batch processing.
    /// </summary>
    public void RecordBucketReference(ulong bucketId, ulong bucketIndex, string storageGuid)
    {
        _bucketRefQueue.Enqueue((bucketId, bucketIndex, storageGuid));
    }

    /// <summary>
    /// Get storage GUID by bucket reference. Returns null if not found.
    /// </summary>
    public async Task<string?> GetStorageGuidByReferenceAsync(ulong bucketId, ulong bucketIndex)
    {
        try
        {
            var key = $"bucket_ref:{bucketId}:{bucketIndex}";
            var value = await _redis.StringGetAsync(key);
            return value.HasValue ? value.ToString() : null;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[Redis] Error getting bucket reference ({bucketId}, {bucketIndex}): {ex.Message}");
            return null;
        }
    }

    /// <summary>
    /// Get all agents that own a specific chunk.
    /// </summary>
    public async Task<List<string>> GetChunkOwnersAsync(string chunkKey)
    {
        // Check cache first
        if (_ownershipCache.TryGetValue(chunkKey, out var cached) && (DateTime.UtcNow - cached.ts).TotalSeconds < 60)
        {
            return cached.agents;
        }

        try
        {
            var key = $"chunk_owner:{chunkKey}";
            var members = await _redis.SetMembersAsync(key);
            var agents = members.Select(m => m.ToString()).Where(a => !string.IsNullOrEmpty(a)).ToList();
            
            // Cache even empty results
            _ownershipCache[chunkKey] = (agents, DateTime.UtcNow);
            return agents;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[Redis] Error getting chunk owners for {chunkKey}: {ex.Message}");
            return new List<string>();
        }
    }

    /// <summary>
    /// Get all active agent pod IPs from Redis (by scanning chunk_owner keys).
    /// </summary>
    public async Task<List<string>> GetActiveAgentPodsAsync()
    {
        try
        {
            var server = _redisConnection.GetServer(_redisConnection.GetEndPoints().First());
            var keys = server.Keys(pattern: "chunk_owner:*", pageSize: 1000);
            
            var agentSet = new HashSet<string>();
            foreach (var key in keys)
            {
                var members = await _redis.SetMembersAsync(key);
                foreach (var member in members)
                {
                    var agent = member.ToString();
                    if (!string.IsNullOrEmpty(agent) && agent != _myPodName)
                    {
                        agentSet.Add(agent);
                    }
                }
            }
            
            return agentSet.ToList();
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[Redis] Error getting active agents: {ex.Message}");
            return new List<string>();
        }
    }

    /// <summary>
    /// Batched flush — runs every 500 ms via timer, adds all queued ownership records to Redis Sets.
    /// </summary>
    private static async Task FlushOwnershipQueueAsync()
    {
        if (_ownershipQueue.IsEmpty || _sharedRedis == null)
            return;

        var batch = new List<(string key, string agent)>();
        while (batch.Count < 500 && _ownershipQueue.TryDequeue(out var item))
            batch.Add(item);

        if (batch.Count == 0)
            return;

        try
        {
            // Group by chunk key to batch Redis operations
            var byChunk = new Dictionary<string, HashSet<string>>();
            foreach (var (chunkKey, agent) in batch)
            {
                if (!byChunk.TryGetValue(chunkKey, out var agents))
                {
                    agents = new HashSet<string>();
                    byChunk[chunkKey] = agents;
                }
                agents.Add(agent);
            }

            // Batch add to Redis Sets using pipelining (fire all commands, then wait)
            var tasks = new List<Task>();
            foreach (var kvp in byChunk)
            {
                var key = $"chunk_owner:{kvp.Key}";
                foreach (var agent in kvp.Value)
                {
                    // Fire all commands without awaiting (pipelining)
                    tasks.Add(_sharedRedis.SetAddAsync(key, agent));
                }
                // Set expiration (24 hours) to prevent stale data
                tasks.Add(_sharedRedis.KeyExpireAsync(key, TimeSpan.FromHours(24)));
            }

            // Wait for all commands to complete
            await Task.WhenAll(tasks);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[Redis] Error flushing ownership queue: {ex.Message}");
            // Re-enqueue so we don't lose data
            foreach (var item in batch)
                _ownershipQueue.Enqueue(item);
        }
    }

    /// <summary>
    /// Batched flush for bucket references — runs every 500 ms via timer.
    /// </summary>
    private static async Task FlushBucketRefQueueAsync()
    {
        if (_bucketRefQueue.IsEmpty || _sharedRedis == null)
            return;

        var batch = new List<(ulong bucketId, ulong bucketIndex, string storageGuid)>();
        while (batch.Count < 500 && _bucketRefQueue.TryDequeue(out var item))
            batch.Add(item);

        if (batch.Count == 0)
            return;

        try
        {
            // Batch Redis SETEX operations using pipelining (fire all commands, then wait)
            var tasks = batch.Select(item =>
            {
                var key = $"bucket_ref:{item.bucketId}:{item.bucketIndex}";
                // Fire command without awaiting (pipelining)
                return _sharedRedis.StringSetAsync(key, item.storageGuid, TimeSpan.FromDays(7)); // 7 day TTL
            }).ToList();

            // Wait for all commands to complete
            await Task.WhenAll(tasks);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[Redis] Error flushing bucket ref queue: {ex.Message}");
            // Re-enqueue so we don't lose data
            foreach (var item in batch)
                _bucketRefQueue.Enqueue(item);
        }
    }
}
