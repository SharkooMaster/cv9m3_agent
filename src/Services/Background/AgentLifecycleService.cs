
using Agent.Services.Agneta;
using Agent.Interfaces.Agneta;
// using Agent.Services.Etcd; // REMOVED: No longer using etcd, using Kubernetes service discovery instead
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
    //private readonly IEtcdClientService _etcdClientService;

    public AgentLifeCycleService()
    {
        //Console.WriteLine("INFO::AgentLifecycleService: Initiating AgentLifeCycleService");
        //_etcdClientService = etcdClientService;
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

    public Task StopAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }
}