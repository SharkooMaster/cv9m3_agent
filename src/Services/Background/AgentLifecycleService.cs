
using Agent.Services.Agneta;
using Agent.Interfaces.Agneta;
using Agent.Services.Etcd;
using Agent.Utils.Misc;
using Agent.Utils.Globals;
using Newtonsoft.Json;
using Agent.Models.Misc;
using Agent.Modules.Agneta;
using System.Numerics;
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
            var nearestNeighbour = await AgnetaHandler.GetNeighbour();
            if(nearestNeighbour.NodeID == "none")
            {
                ServiceData _self_sd = JsonConvert.DeserializeObject<ServiceData>(_data);

                // first node
                for (int i = 0; i < Globals.FINGER_TABLE_SIZE; i++)
                {
                    var jumpSize = 1UL << i;
                    var target = new Vector2(jumpSize, jumpSize);
                    Globals.DHT_NODE.FingerTable.Add(target, _self_sd);
                }
            }
            else
            {
                ServiceData neighbourData = JsonConvert.DeserializeObject<ServiceData>(nearestNeighbour.Data);
            }
            Console.WriteLine(nearestNeighbour);
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