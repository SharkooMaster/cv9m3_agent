
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
        await Task.Run(() => _appLifetime.ApplicationStarted.WaitHandle.WaitOne());
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
            string bootstrap_node = null;

            if(!AgnetaHandler.disabled)
            {
                var nearestNeighbour = await AgnetaHandler.GetNeighbour();
                if(nearestNeighbour.NodeID != "none")
                {
                    ServiceData neighbourData = JsonConvert.DeserializeObject<ServiceData>(nearestNeighbour.Data);
                    if(neighbourData.Host != Globals._NODE.ip)
                    {
                        Console.WriteLine("found peer");
                        bootstrap_node = neighbourData.Host;
                    }
                }
            }
            else
            {
                // Use headless service
                var addresses = await Dns.GetHostAddressesAsync("agent-headless.cross-test.svc.cluster.local");
                var myIp = Environment.GetEnvironmentVariable("MY_POD_IP");
                Console.WriteLine($"pod ip: {myIp}");

                var peerAddresses = addresses.Where(ip => ip.ToString() != myIp).ToList();

                if (peerAddresses.Count == 0)
                {
                    Console.WriteLine("No agents found. Starting standalone");
                }
                else
                {
                    Console.WriteLine($"Found {peerAddresses.Count} other agents. Checking reachability...");
                    foreach (var ip in peerAddresses.OrderBy(_ => Guid.NewGuid())) // randomize order
                    {
                        if (await IsAgentReachable(ip.ToString()))
                        {
                            Console.WriteLine($"Selected reachable peer: {ip}");
                            bootstrap_node = ip.ToString();
                            break;
                        }
                    }

                    if (bootstrap_node == null)
                    {
                        Console.WriteLine("No reachable agents found. Starting standalone.");
                    }
                }
            }

            await BackgrounfServiceManager.RegisterFireMethod("JoinCluster", async () => {
                Globals._NODE = await NodeService.JoinNetwork(Globals._NODE, bootstrap_node);
            });
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