
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
    //private readonly IEtcdClientService _etcdClientService;
    private readonly IHostApplicationLifetime _appLifetime;

    public AgentLifeCycleService(IHostApplicationLifetime appLifetime)
    {
        _appLifetime = appLifetime;
        //Console.WriteLine("INFO::AgentLifecycleService: Initiating AgentLifeCycleService");
        //_etcdClientService = etcdClientService;
    }

    public static async Task<bool> IsAgentReachable(string ip, int port = 5000)
    {
        try
        {
            using var client = new TcpClient();
            var connectTask = client.ConnectAsync(ip, port);
            var timeoutTask = Task.Delay(2000); // 2 seconds timeout

            var completedTask = await Task.WhenAny(connectTask, timeoutTask);
            return completedTask == connectTask && client.Connected;
        }
        catch
        {
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

        Globals._NODE.ip = Misc.GetLocalIPAddress();
        Globals._NODE.id = await NodeUtils.generateNodeID();

        // Getting target neighbor
        try
        {
            if(!AgnetaHandler.disabled)
            {
                var nearestNeighbour = await AgnetaHandler.GetNeighbour();
                if(nearestNeighbour.NodeID != "none")
                {
                    ServiceData neighbourData = JsonConvert.DeserializeObject<ServiceData>(nearestNeighbour.Data);
                    if(neighbourData.Host != Globals._NODE.ip)
                    {
                        Console.WriteLine("found peer");
                        Globals.bootstrap_node = neighbourData.Host;
                    }
                }
            }
            else
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
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Unexpected error: {ex.GetType().Name}: {ex.Message}");
                }
            }
        }
        catch(Exception ex)
        {
            Console.WriteLine($"ERROR::AgentLifeCycleService: {ex.Data} : {ex.Message}");
        }
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }
}