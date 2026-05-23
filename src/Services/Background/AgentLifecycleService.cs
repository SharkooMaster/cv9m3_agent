
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
    private Task? _etcdHeartbeatTask;

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
                var entry = new
                {
                    name = podName,
                    ip = podIp,
                    port = 5000,
                    vnodes = vnodesPerAgent,
                    version = 2,
                    status = "ready"
                };
                string json = System.Text.Json.JsonSerializer.Serialize(entry);

                _etcdLeaseId = await _etcdClientService.RegisterAgentLeaseAsync(podName, json);
                Console.WriteLine(
                    $"[AgentLifecycleService] Registered in etcd /agents/{podName} → {json} (leaseId={_etcdLeaseId})");

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
                            // hiccup doesn't permanently de-list us.
                            await _etcdClientService.UpdateHeartBeatAsync(_etcdLeaseId, _etcdCts.Token);

                            if (_etcdCts.IsCancellationRequested) return;

                            Console.WriteLine("[AgentLifecycleService] etcd keep-alive stream ended; re-registering");
                            _etcdLeaseId = await _etcdClientService.RegisterAgentLeaseAsync(podName, json);
                        }
                        catch (OperationCanceledException) { return; }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"[AgentLifecycleService] etcd heartbeat error: {ex.Message}; retrying in 3s");
                            try { await Task.Delay(TimeSpan.FromSeconds(3), _etcdCts.Token); }
                            catch (OperationCanceledException) { return; }
                            try
                            {
                                _etcdLeaseId = await _etcdClientService.RegisterAgentLeaseAsync(podName, json);
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
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[AgentLifecycleService] etcd deregister failed: {ex.Message}");
        }
    }
}
