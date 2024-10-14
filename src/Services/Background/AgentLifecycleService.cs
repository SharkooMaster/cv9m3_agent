
using Agent.Services.Agneta;
using Agent.Interfaces.Agneta;
using Agent.Services.Etcd;
using Agent.Utils.Misc;
using Newtonsoft.Json;
using Agent.Models.Misc;
namespace Agent.Services;

public class AgentLifeCycleService : IHostedService
{
    //private readonly IEtcdClientService _etcdClientService;
    private readonly IAgnetaClientService _agnetaClientService;

    public AgentLifeCycleService(IAgnetaClientService agnetaClientService)
    {
        Console.WriteLine("INFO::AgentLifecycleService: Initiating AgentLifeCycleService");
        //_etcdClientService = etcdClientService;
        _agnetaClientService = agnetaClientService;
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
            var nearestNeighbour = await _agnetaClientService.GetAssignedNeighbour();
            ServiceData neighbourData = JsonConvert.DeserializeObject<ServiceData>(nearestNeighbour.Data);
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