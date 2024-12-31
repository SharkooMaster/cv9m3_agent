
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
namespace Agent.Services;

public class AgentLifeCycleService : IHostedService
{
    //private readonly IEtcdClientService _etcdClientService;
    public AgentLifeCycleService()
    {
        Console.WriteLine("INFO::AgentLifecycleService: Initiating AgentLifeCycleService");
        //_etcdClientService = etcdClientService;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        string _id = Misc.GenerateId();
        string _data = Misc.GetServiceInfo("agent", _id);

        // Assigning to global variables
        Globals.ETCD_ID = _id;
        Globals.ETCD_VALUE = _data;

        // Getting target neighbor
        try
        {
            string bootstrap_node = "";
            var nearestNeighbour = await AgnetaHandler.GetNeighbour();
            if(nearestNeighbour.NodeID != "none")
            {
                ServiceData neighbourData = JsonConvert.DeserializeObject<ServiceData>(nearestNeighbour.Data);
                if(neighbourData.Host != Globals._NODE.ip)
                {
                    bootstrap_node = neighbourData.Host;
                }
            }
            Console.WriteLine(nearestNeighbour);

            await NodeService.JoinNetwork(Globals._NODE, bootstrap_node);
        }
        catch
        {
            Console.WriteLine("ERROR::AgentLifeCycleService: Could not read propery (host) on aquired neighbour");
        }
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }
}