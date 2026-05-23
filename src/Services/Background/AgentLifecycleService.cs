
using Agent.Services.Agneta;
using Agent.Interfaces.Agneta;
using Agent.Services.Etcd;
using Agent.Utils.Misc;
using Agent.Utils.Globals;
using Newtonsoft.Json;
using Agent.Models.Misc;
using Agent.Modules.Agneta;
using System.Numerics;
using Agent.Modules.Peer;
using Agent.Utils;
using System.Net;
using System.Net.Sockets;
namespace Agent.Services;

public class AgentLifeCycleService : IHostedService
{
    private static readonly Random _random = new Random();
    private readonly IEtcdClientService? _etcdClientService;
    private CancellationTokenSource? _etcdCts;
    private long _etcdLeaseId = 0;
    private string? _etcdAgentId;
    private string? _etcdAgentIp;
    private int _etcdVnodes = 256;
    private Task? _etcdHeartbeatTask;

    /// <summary>
    /// Current self-published status. Mutated by <see cref="MarkRetiringAsync"/>
    /// when the pod is told to drain (preStop hook → POST /admin/retire). The
    /// heartbeat loop reads this every time it (re)registers so the etcd
    /// entry's <c>status</c> field stays in sync — the gateway's rebalance
    /// coordinator watches for the <c>draining</c> transition and starts
    /// orchestrating handoffs.
    /// </summary>
    private volatile string _currentStatus = "ready";

    /// <summary>
    /// Singleton handle so the admin retire endpoint and other hosted
    /// services can call into the lifecycle service without going through
    /// DI lookup tricks. Set during <see cref="StartAsync"/> and cleared
    /// in <see cref="StopAsync"/>.
    /// </summary>
    public static AgentLifeCycleService? Instance { get; private set; }

    public bool IsRetiring => _currentStatus == "draining";
    public string? AgentId => _etcdAgentId;

    public AgentLifeCycleService(IServiceProvider provider)
    {
        // Resolve IEtcdClientService optionally — when ETCD_ENDPOINT is unset
        // the service is never registered in DI, GetService returns null, and
        // the agent runs in legacy mode (no etcd self-registration; cross
        // falls back to DNS+GetNodeInfo routing).
        _etcdClientService = provider.GetService(typeof(IEtcdClientService)) as IEtcdClientService;
    }

    public static async Task<bool> IsAgentReachable(string ip, int port = 5000)
    {
        // Use Task.Run to isolate DNS/connection failures and ensure all exceptions are observed
        var reachableTask = Task.Run(async () =>
        {
            try
            {
                using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
                using var client = new TcpClient();
                await client.ConnectAsync(ip, port, cts.Token).ConfigureAwait(false);
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

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        Instance = this;
        string _id = Misc.GenerateId();
        string _data = Misc.GetServiceInfo("agent", _id);

        // Assigning to global variables
        Globals.ETCD_ID = _id;
        Globals.ETCD_VALUE = _data;

        Globals._NODE.ip = Environment.GetEnvironmentVariable("MY_POD_IP");
        Globals._NODE.id = await NodeUtils.generateNodeID();

        // ── Etcd self-registration for RendezvousRouter consumption ──
        // Cross's RendezvousRouter watches `/agents/` in etcd to build a
        // *consistent* membership view across all cross pods. Without this,
        // each cross pod independently does DNS + GetNodeInfo polling, which
        // can drift by up to 15s and cause two cross pods to route the same
        // (BucketId, BucketKey) to different agents → silent corruption at
        // decompress time.
        //
        // The key MUST be MY_POD_NAME (stable across StatefulSet pod
        // reschedules) and MUST match the routing identity that
        // GetNodeInfo returns, so cross sees the same name whether it
        // consumes via etcd or via the legacy DNS+GetNodeInfo fallback.
        //
        // The value carries the IP so cross can connect without an extra
        // GetNodeInfo round trip. The 10s lease auto-cleans the entry if
        // this pod dies; the heartbeat loop keeps it alive while we're
        // running.
        if (_etcdClientService != null)
        {
            try
            {
                string podName =
                    Environment.GetEnvironmentVariable("MY_POD_NAME")
                    ?? Environment.GetEnvironmentVariable("MY_NODE_NAME")
                    ?? _id;
                string podIp =
                    Environment.GetEnvironmentVariable("MY_POD_IP")
                    ?? "127.0.0.1";

                _etcdAgentId = podName;
                _etcdAgentIp = podIp;
                // ── /agents/<podName> v2 schema ──
                // version=1: legacy {name, ip, port} consumed by the old
                //   RendezvousRouter on cross. Still emitted here so a
                //   newer agent registering against a cluster with older
                //   cross/gateway pods doesn't break their parsing.
                // version=2: adds vnodes (per-pod vnode count for the
                //   consistent-hash ring) and status (ready/warming/
                //   draining — used by the rebalance coordinator to know
                //   which agents can accept writes).
                //
                // The watcher on cross/gateway falls back to defaults when
                // any of the new fields are missing, so this is safe to
                // ship ahead of the consumer-side migration.
                int vnodesPerAgent = 256;
                var vnodesEnv = Environment.GetEnvironmentVariable("VNODES_PER_AGENT");
                if (!string.IsNullOrWhiteSpace(vnodesEnv) && int.TryParse(vnodesEnv, out var vEnv) && vEnv > 0)
                {
                    vnodesPerAgent = vEnv;
                }
                _etcdVnodes = vnodesPerAgent;

                _etcdLeaseId = await _etcdClientService.RegisterAgentLeaseAsync(podName, BuildEtcdEntryJson());
                Console.WriteLine(
                    $"[AgentLifecycleService] Registered in etcd /agents/{podName} → {BuildEtcdEntryJson()} (leaseId={_etcdLeaseId})");

                _etcdCts = new CancellationTokenSource();
                _etcdHeartbeatTask = Task.Run(async () =>
                {
                    while (!_etcdCts.IsCancellationRequested)
                    {
                        try
                        {
                            // LeaseKeepAlive blocks until the token cancels or
                            // the keep-alive stream breaks. On stream break we
                            // re-register a fresh lease so a transient etcd
                            // hiccup doesn't permanently de-list us. The JSON
                            // is rebuilt each time so a status flip (e.g.
                            // ready → draining via /admin/retire) is reflected
                            // automatically on the next heartbeat-bounce.
                            await _etcdClientService.UpdateHeartBeatAsync(_etcdLeaseId, _etcdCts.Token);

                            if (_etcdCts.IsCancellationRequested) return;

                            Console.WriteLine($"[AgentLifecycleService] etcd keep-alive stream ended; re-registering with status={_currentStatus}");
                            _etcdLeaseId = await _etcdClientService.RegisterAgentLeaseAsync(podName, BuildEtcdEntryJson());
                        }
                        catch (OperationCanceledException) { return; }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"[AgentLifecycleService] etcd heartbeat error: {ex.Message}; retrying in 3s");
                            try { await Task.Delay(TimeSpan.FromSeconds(3), _etcdCts.Token); }
                            catch (OperationCanceledException) { return; }
                            try
                            {
                                _etcdLeaseId = await _etcdClientService.RegisterAgentLeaseAsync(podName, BuildEtcdEntryJson());
                            }
                            catch (Exception rex)
                            {
                                Console.WriteLine($"[AgentLifecycleService] etcd re-register failed: {rex.Message}");
                            }
                        }
                    }
                });
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[AgentLifecycleService] etcd registration failed (continuing without it): {ex.Message}");
            }
        }
        else
        {
            Console.WriteLine("[AgentLifecycleService] etcd client not configured (ETCD_ENDPOINT unset); skipping membership self-registration");
        }

        // LOCAL MODE: Skip bootstrap discovery if in local mode
        bool isLocalMode = Agent.Utils.LocalModeDetector.IsLocalMode();
        Console.WriteLine($"[AgentLifecycleService] Local mode check: {isLocalMode}");
        if (isLocalMode)
        {
            Console.WriteLine("[AgentLifecycleService] Local mode detected - skipping all bootstrap discovery");
            Globals.bootstrap_node = null; // Ensure bootstrap_node is null in local mode
            return; // Exit early, don't try to discover other agents
        }

        // Getting target neighbor
        try
        {
            try
            {
                var addresses = await Dns.GetHostAddressesAsync("agent-headless.cross-test.svc.cluster.local");
                Console.WriteLine($"Resolved {addresses.Length} addresses: {string.Join<IPAddress>(", ", addresses)}");

                if (addresses == null || addresses.Length == 0)
                {
                    Console.WriteLine("DNS lookup succeeded, but no agents found.");
                    // No peers at all, continue standalone
                }
                else
                {
                    var myIp = Environment.GetEnvironmentVariable("MY_POD_IP");
                    Console.WriteLine($"pod ip: {myIp}");

                    var peerAddresses = addresses.Where(ip => ip.ToString() != myIp).ToList();

                    if (peerAddresses.Count == 0)
                    {
                        Console.WriteLine("No other agents available (excluding self). Starting standalone.");
                    }
                    else
                    {
                        Console.WriteLine($"Found {peerAddresses.Count} other agents. Checking reachability...");
                        foreach (var ip in peerAddresses.OrderBy(_ => Guid.NewGuid())) // randomize order
                        {
                            if (await IsAgentReachable(ip.ToString()))
                            {
                                Console.WriteLine($"Selected reachable peer: {ip}");
                                Globals.bootstrap_node = ip.ToString();
                                break;
                            }
                        }

                        if (Globals.bootstrap_node == null)
                        {
                            Console.WriteLine("No reachable agents found. Starting standalone.");
                        }
                    }
                }
            }
            catch (SocketException se)
            {
                Console.WriteLine($"DNS lookup failed ({se.SocketErrorCode}): {se.Message}");
                // Fallback: Try Docker service names for local deployment
                await TryDockerBootstrapAsync();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Unexpected error: {ex.GetType().Name}: {ex.Message}");
                // Fallback: Try Docker service names for local deployment
                await TryDockerBootstrapAsync();
            }
            
            // If still no bootstrap node found, try Docker discovery
            if (Globals.bootstrap_node == null)
            {
                await TryDockerBootstrapAsync();
            }
        }
        catch(Exception ex)
        {
            Console.WriteLine($"ERROR::AgentLifeCycleService: {ex.Data} : {ex.Message}");
        }
    }
    
    /// <summary>
    /// Docker-specific bootstrap: Try to discover agents using known Docker service names.
    /// </summary>
    private static async Task TryDockerBootstrapAsync()
    {
        // LOCAL MODE: Skip Docker bootstrap if in local mode
        if (Agent.Utils.LocalModeDetector.IsLocalMode())
        {
            Console.WriteLine("[Docker Bootstrap] Local mode detected - skipping Docker bootstrap");
            return;
        }
        
        try
        {
            var myIp = Environment.GetEnvironmentVariable("MY_POD_IP");
            Console.WriteLine($"[Docker Bootstrap] MY_POD_IP: {myIp}");
            
            // Known Docker service names for agents
            var knownAgents = new[] { "agent-1", "agent-2", "agent-3" };
            
            foreach (var agentName in knownAgents)
            {
                // Skip self
                if (agentName == myIp)
                    continue;
                
                Console.WriteLine($"[Docker Bootstrap] Trying to reach {agentName}...");
                if (await IsAgentReachable(agentName))
                {
                    Console.WriteLine($"[Docker Bootstrap] ✅ Found reachable agent: {agentName}");
                    Globals.bootstrap_node = agentName;
                    return;
                }
            }
            
            Console.WriteLine("[Docker Bootstrap] No reachable agents found via Docker service names.");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[Docker Bootstrap] Error during Docker discovery: {ex.Message}");
        }
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        // Best-effort etcd de-registration: revoke the lease so cross's
        // RendezvousRouter sees us disappear immediately instead of waiting
        // for the 10s TTL.
        try
        {
            _etcdCts?.Cancel();
            if (_etcdClientService != null && _etcdLeaseId != 0 && !string.IsNullOrEmpty(_etcdAgentId))
            {
                await _etcdClientService.DeregisterAgentLeaseAsync(_etcdAgentId, _etcdLeaseId);
                Console.WriteLine($"[AgentLifecycleService] Deregistered /agents/{_etcdAgentId} from etcd");
            }
            if (_etcdHeartbeatTask != null)
            {
                try { await Task.WhenAny(_etcdHeartbeatTask, Task.Delay(TimeSpan.FromSeconds(3))); }
                catch { /* shutdown best-effort */ }
            }
            Instance = null;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[AgentLifecycleService] etcd deregister failed: {ex.Message}");
        }
    }

    /// <summary>
    /// Build the etcd value emitted under <c>/agents/&lt;podName&gt;</c>.
    /// Captured fields are stable for the lifetime of the pod; only
    /// <see cref="_currentStatus"/> mutates (ready → draining when the
    /// retire endpoint fires).
    /// </summary>
    private string BuildEtcdEntryJson()
    {
        var entry = new
        {
            name = _etcdAgentId,
            ip = _etcdAgentIp ?? "127.0.0.1",
            port = 5000,
            vnodes = _etcdVnodes,
            version = 2,
            status = _currentStatus
        };
        return System.Text.Json.JsonSerializer.Serialize(entry);
    }

    /// <summary>
    /// Transition this pod into the "draining" state.
    ///
    /// Mechanics:
    ///   1. Flip <see cref="_currentStatus"/> to "draining".
    ///   2. Revoke the current etcd lease. This causes the keep-alive
    ///      stream to end; the heartbeat loop falls into its catch path
    ///      and re-registers immediately, picking up the new status from
    ///      <see cref="BuildEtcdEntryJson"/>.
    ///   3. The gateway's <c>EtcdMembershipWatcher</c> sees the value
    ///      change and bumps <c>RingState.TopologyVersion</c>; the
    ///      rebalance coordinator then orchestrates handoffs of this
    ///      pod's vnodes to surviving peers.
    ///
    /// Returns <c>true</c> if the status flip was applied (etcd was
    /// configured), <c>false</c> if etcd is unwired (legacy/local mode).
    ///
    /// Idempotent — calling twice is safe; the second call observes
    /// <see cref="IsRetiring"/> already true and short-circuits.
    /// </summary>
    public async Task<bool> MarkRetiringAsync()
    {
        if (_etcdClientService == null || string.IsNullOrEmpty(_etcdAgentId))
        {
            Console.WriteLine("[AgentLifecycleService] MarkRetiringAsync: etcd unwired or no agent id; cannot drain");
            return false;
        }

        if (_currentStatus == "draining")
        {
            Console.WriteLine("[AgentLifecycleService] MarkRetiringAsync: already draining (no-op)");
            return true;
        }

        _currentStatus = "draining";
        Console.WriteLine($"[AgentLifecycleService] MarkRetiringAsync: status flipped to 'draining' for /agents/{_etcdAgentId}");

        // Revoke + delete the current key so the heartbeat loop's
        // catch path re-registers. We deliberately use the existing
        // Deregister API (which both revokes the lease AND deletes the
        // key); the brief gap before re-register is harmless because
        // the gateway watcher debounces ring rebuilds and we'll come
        // back as 'draining' within a few hundred ms.
        if (_etcdLeaseId != 0)
        {
            try
            {
                await _etcdClientService.DeregisterAgentLeaseAsync(_etcdAgentId, _etcdLeaseId);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[AgentLifecycleService] MarkRetiringAsync: lease revoke failed: {ex.Message}");
            }
        }

        // Force-register immediately rather than wait for the heartbeat
        // loop to retry — the gateway must see the draining status
        // before we start polling for adoption completion below.
        try
        {
            _etcdLeaseId = await _etcdClientService.RegisterAgentLeaseAsync(_etcdAgentId, BuildEtcdEntryJson());
            Console.WriteLine($"[AgentLifecycleService] MarkRetiringAsync: re-registered with status=draining (leaseId={_etcdLeaseId})");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[AgentLifecycleService] MarkRetiringAsync: re-register failed: {ex.Message}");
            return false;
        }

        return true;
    }
}
