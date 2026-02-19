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
    private static readonly Timer _ownershipFlushTimer = new(_ => FlushOwnershipQueueAsync().ConfigureAwait(false), null, 500, 500);
    private static IDatabase? _sharedRedis;

    // ── Ownership read cache (chunkKey → list of agent IPs) ──
    private static readonly ConcurrentDictionary<string, (List<string> agents, DateTime ts)> _ownershipCache = new();

    public RedisChunkOwnershipService(string redisHost, int redisPort)
    {
        var config = ConfigurationOptions.Parse($"{redisHost}:{redisPort}");
        config.AbortOnConnectFail = false;
        config.ConnectRetry = 3;
        config.ConnectTimeout = 5000;
        config.SyncTimeout = 5000;
        
        _redisConnection = ConnectionMultiplexer.Connect(config);
        _redis = _redisConnection.GetDatabase();
        _sharedRedis = _redis;
        _myPodName = Environment.GetEnvironmentVariable("MY_POD_IP") ?? Environment.GetEnvironmentVariable("MY_POD_NAME") ?? "unknown";
        
        Console.WriteLine($"[Redis] Connected to {redisHost}:{redisPort}, pod={_myPodName}");
    }

    public void Dispose()
    {
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

            // Batch add to Redis Sets
            var tasks = byChunk.Select(async kvp =>
            {
                var key = $"chunk_owner:{kvp.Key}";
                foreach (var agent in kvp.Value)
                {
                    await _sharedRedis.SetAddAsync(key, agent);
                }
                // Set expiration (24 hours) to prevent stale data
                await _sharedRedis.KeyExpireAsync(key, TimeSpan.FromHours(24));
            });

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
}
