using Agent.Modules.Storage;
using Agent.Services.Etcd;
using Etcdserverpb;
using Google.Protobuf;
using Grpc.Net.Client;
using System.Text.Json;

namespace Agent.Services.Background;

/// <summary>
/// Periodic Merkle-based anti-entropy: every few minutes, this agent
/// asks each peer in the cluster for its 256-bucket Merkle summary of
/// the chunk-store, compares it to its own, and surfaces any buckets
/// that disagree. When auto-repair is enabled, the agent then triggers
/// <c>BeginVnodeAdoption</c> against the peer it's behind to pull
/// the differing chunks.
///
/// Why we need this on top of the W=2 quorum and the rebalance
/// coordinator:
///   - A write that succeeded on 2/3 replicas leaves the 3rd silently
///     behind. Subsequent reads can still find it on the 2 ahead, but
///     if one of those 2 dies before any further write, the chunk is
///     reduced to a single replica → next disk failure loses the data.
///   - Network partitions during a rebalance leave partial vnode
///     handoffs.
///   - Manual <c>kubectl delete pod</c> in dev surfaces the same
///     scenario.
///
/// Anti-entropy heals these silent drifts on a steady cadence without
/// needing to trace the original write that caused them.
///
/// Disabled by default for v1 — set <c>ANTI_ENTROPY_ENABLED=true</c>.
/// We keep auto-repair *also* opt-in (<c>ANTI_ENTROPY_AUTO_REPAIR=true</c>)
/// because the first deployment will want to observe divergence
/// reports before automatically pulling data around.
/// </summary>
public class AntiEntropyService : IHostedService, IDisposable
{
    private readonly IServiceProvider _provider;
    private CancellationTokenSource? _cts;
    private Task? _runner;

    public AntiEntropyService(IServiceProvider provider)
    {
        _provider = provider;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        if (!IsEnabled())
        {
            Console.WriteLine("[AntiEntropyService] disabled (ANTI_ENTROPY_ENABLED != true) — skipping");
            return Task.CompletedTask;
        }
        Console.WriteLine("[AntiEntropyService] starting; interval = " + IntervalSeconds() + "s, auto-repair = " + AutoRepairEnabled());
        _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _runner = Task.Run(() => RunAsync(_cts.Token));
        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        try
        {
            _cts?.Cancel();
            if (_runner != null)
                await Task.WhenAny(_runner, Task.Delay(TimeSpan.FromSeconds(3), cancellationToken));
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[AntiEntropyService] StopAsync error: {ex.Message}");
        }
    }

    private async Task RunAsync(CancellationToken token)
    {
        // Initial delay so the agent finishes warming up (RocksDB stats
        // scan, bucket cache load) before competing for IO with peer
        // anti-entropy traffic. 60s is conservative; tune via
        // ANTI_ENTROPY_INITIAL_DELAY_SEC if needed.
        try { await Task.Delay(TimeSpan.FromSeconds(GetInitialDelaySeconds()), token); }
        catch (OperationCanceledException) { return; }

        var interval = TimeSpan.FromSeconds(IntervalSeconds());
        while (!token.IsCancellationRequested)
        {
            try
            {
                await OneRoundAsync(token);
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested) { return; }
            catch (Exception ex)
            {
                Console.WriteLine($"[AntiEntropyService] round error: {ex.GetType().Name}: {ex.Message}");
            }

            try { await Task.Delay(interval, token); }
            catch (OperationCanceledException) { return; }
        }
    }

    private async Task OneRoundAsync(CancellationToken token)
    {
        var peers = await DiscoverPeersAsync(token);
        if (peers.Count == 0)
        {
            Console.WriteLine("[AntiEntropyService] no peers known — skipping round");
            return;
        }

        // Compute self summary once.
        var selfSummary = ComputeSelfSummary();
        Console.WriteLine($"[AntiEntropyService] self summary: {selfSummary.chunkCount} chunks across 256 buckets");

        foreach (var (podName, ip) in peers)
        {
            if (token.IsCancellationRequested) return;
            try
            {
                await ComparePeerAsync(podName, ip, selfSummary, token);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[AntiEntropyService] compare with {podName} failed: {ex.Message}");
            }
        }
    }

    private async Task ComparePeerAsync(
        string peerPod,
        string peerIp,
        (byte[] summary, ulong chunkCount) self,
        CancellationToken token)
    {
        var address = peerIp.Contains("://") ? peerIp : $"http://{peerIp}:5000";
        using var channel = GrpcChannel.ForAddress(address, new GrpcChannelOptions
        {
            Credentials = global::Grpc.Core.ChannelCredentials.Insecure,
            MaxReceiveMessageSize = 256 * 1024 * 1024,
            MaxSendMessageSize = 256 * 1024 * 1024
        });
        var client = new VnodeStreamingService.VnodeStreamingServiceClient(channel);

        var peerRes = await client.GetVnodeMerkleAsync(
            new GetVnodeMerkle_Req { HashRangeLow = 0, HashRangeHigh = uint.MaxValue },
            deadline: DateTime.UtcNow.AddSeconds(60),
            cancellationToken: token);

        var peerBytes = peerRes.Summary?.ToByteArray() ?? Array.Empty<byte>();
        if (peerBytes.Length != 256 * 32)
        {
            Console.WriteLine($"[AntiEntropyService] {peerPod} returned summary of {peerBytes.Length} bytes (expected 8192) — skipping");
            return;
        }

        // Compare bucket-by-bucket (32 bytes each) and count divergence.
        int divergent = 0;
        for (int i = 0; i < 256; i++)
        {
            int off = i * 32;
            for (int b = 0; b < 32; b++)
            {
                if (self.summary[off + b] != peerBytes[off + b])
                {
                    divergent++;
                    break;
                }
            }
        }

        Console.WriteLine(
            $"[AntiEntropyService] {peerPod} diff: {divergent}/256 buckets differ " +
            $"(self chunks={self.chunkCount}, peer chunks={peerRes.ChunkCount})");

        if (divergent == 0) return;

        // Auto-repair: trigger a stream pull from the peer if peer has
        // strictly more chunks (we're behind). Symmetric repair (peer
        // pulls from us) happens on the peer's own anti-entropy round.
        if (AutoRepairEnabled() && peerRes.ChunkCount > self.chunkCount)
        {
            Console.WriteLine($"[AntiEntropyService] {peerPod} has more chunks — triggering self-pull");
            try
            {
                var myPod = Environment.GetEnvironmentVariable("MY_POD_NAME") ?? string.Empty;
                // Self-trigger: we tell *ourselves* to BeginVnodeAdoption
                // from the peer. Routed through localhost so the
                // adoption uses the existing server-side machinery.
                var localChannel = GrpcChannel.ForAddress("http://localhost:5000", new GrpcChannelOptions
                {
                    Credentials = global::Grpc.Core.ChannelCredentials.Insecure,
                    MaxReceiveMessageSize = 256 * 1024 * 1024,
                    MaxSendMessageSize = 256 * 1024 * 1024
                });
                var localClient = new VnodeStreamingService.VnodeStreamingServiceClient(localChannel);
                var res = await localClient.BeginVnodeAdoptionAsync(new BeginVnodeAdoption_Req
                {
                    SourcePod = peerPod,
                    SourceIp = peerIp,
                    HashRangeLow = 0,
                    HashRangeHigh = uint.MaxValue,
                    TopologyVersion = 0
                }, cancellationToken: token);
                Console.WriteLine($"[AntiEntropyService] adoption_id={res.AdoptionId} accepted={res.Accepted}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[AntiEntropyService] auto-repair pull from {peerPod} failed: {ex.Message}");
            }
        }
    }

    private static (byte[] summary, ulong chunkCount) ComputeSelfSummary()
    {
        // We re-implement the same algorithm the gRPC server uses
        // (in VnodeStreamingGrpcService.GetVnodeMerkle) so the in-pod
        // self summary doesn't need to do an in-process gRPC call.
        const int Buckets = 256;
        var bucketGuids = new List<string>[Buckets];
        for (int i = 0; i < Buckets; i++) bucketGuids[i] = new List<string>();

        ulong total = 0;
        foreach (var (storageGuid, _) in NetworkFileStorageHandler.EnumerateAllChunks())
        {
            if (string.IsNullOrEmpty(storageGuid) || storageGuid.Length < 2) continue;
            int bucket;
            try { bucket = Convert.ToInt32(storageGuid.Substring(0, 2), 16); }
            catch { continue; }
            bucketGuids[bucket].Add(storageGuid);
            total++;
        }

        var summary = new byte[Buckets * 32];
        for (int i = 0; i < Buckets; i++)
        {
            var list = bucketGuids[i];
            if (list.Count == 0) continue;
            list.Sort(StringComparer.Ordinal);

            using var sha = System.Security.Cryptography.SHA256.Create();
            var sb = new System.Text.StringBuilder(list.Count * 65);
            foreach (var g in list) { sb.Append(g); sb.Append('\n'); }
            var hash = sha.ComputeHash(System.Text.Encoding.UTF8.GetBytes(sb.ToString()));
            hash.AsSpan().CopyTo(new Span<byte>(summary, i * 32, 32));
        }
        return (summary, total);
    }

    private async Task<List<(string podName, string ip)>> DiscoverPeersAsync(CancellationToken token)
    {
        // Cheapest discovery available: read /agents/ from etcd directly.
        // We don't keep the agent's internal ring snapshot here — agent
        // is the source of truth for *which agents exist* via etcd self-
        // registration, but doesn't currently maintain a local ring.
        var endpoint = Environment.GetEnvironmentVariable("ETCD_ENDPOINT");
        if (string.IsNullOrWhiteSpace(endpoint))
        {
            Console.WriteLine("[AntiEntropyService] ETCD_ENDPOINT unset — peer discovery disabled");
            return new();
        }

        try
        {
            using var etcd = new dotnet_etcd.EtcdClient(
                connectionString: endpoint,
                configureChannelOptions: opts => { opts.Credentials = global::Grpc.Core.ChannelCredentials.Insecure; });

            var resp = await etcd.GetAsync(new RangeRequest
            {
                Key = ByteString.CopyFromUtf8("/agents/"),
                RangeEnd = ByteString.CopyFromUtf8("/agents0"),
            }, cancellationToken: token);

            var myPod = Environment.GetEnvironmentVariable("MY_POD_NAME");
            var peers = new List<(string, string)>();
            foreach (var kv in resp.Kvs)
            {
                var name = kv.Key.ToStringUtf8().Replace("/agents/", "");
                if (string.Equals(name, myPod, StringComparison.Ordinal)) continue;

                try
                {
                    using var doc = JsonDocument.Parse(kv.Value.ToStringUtf8());
                    if (doc.RootElement.TryGetProperty("ip", out var ipEl))
                    {
                        var ip = ipEl.GetString();
                        if (!string.IsNullOrEmpty(ip)) peers.Add((name, ip));
                    }
                }
                catch { /* skip malformed entry */ }
            }
            return peers;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[AntiEntropyService] etcd peer discovery failed: {ex.Message}");
            return new();
        }
    }

    private static bool IsEnabled() =>
        string.Equals(Environment.GetEnvironmentVariable("ANTI_ENTROPY_ENABLED"), "true", StringComparison.OrdinalIgnoreCase)
        || Environment.GetEnvironmentVariable("ANTI_ENTROPY_ENABLED") == "1";

    private static bool AutoRepairEnabled() =>
        string.Equals(Environment.GetEnvironmentVariable("ANTI_ENTROPY_AUTO_REPAIR"), "true", StringComparison.OrdinalIgnoreCase);

    private static int IntervalSeconds()
    {
        var raw = Environment.GetEnvironmentVariable("ANTI_ENTROPY_INTERVAL_SEC");
        if (int.TryParse(raw, out var v) && v >= 60) return v;
        return 300;  // 5 minutes default
    }

    private static int GetInitialDelaySeconds()
    {
        var raw = Environment.GetEnvironmentVariable("ANTI_ENTROPY_INITIAL_DELAY_SEC");
        if (int.TryParse(raw, out var v) && v >= 0) return v;
        return 60;
    }

    public void Dispose() { _cts?.Dispose(); }
}
