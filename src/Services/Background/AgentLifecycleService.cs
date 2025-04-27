
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
namespace Agent.Services;

public class AgentLifeCycleService : IHostedService
{
    //private readonly IEtcdClientService _etcdClientService;
    public AgentLifeCycleService()
    {
        //Console.WriteLine("INFO::AgentLifecycleService: Initiating AgentLifeCycleService");
        //_etcdClientService = etcdClientService;
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

                var peerAddresses = addresses.Where(ip => ip.ToString() != myIp).ToList();

                if (peerAddresses.Count == 0)
                {
                    Console.WriteLine("No agents found.");
                }
                else
                {
                    var random = new Random();
                    var selectedIp = peerAddresses[random.Next(peerAddresses.Count)];

                    Console.WriteLine($"Randomly selected agent: {selectedIp}");
                    bootstrap_node = selectedIp.ToString();
                }
            }

            Globals._NODE = await NodeService.JoinNetwork(Globals._NODE, bootstrap_node);
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