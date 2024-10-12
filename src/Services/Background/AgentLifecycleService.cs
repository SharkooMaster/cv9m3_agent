
using Agent.Services.Agneta;
using Agent.Services.Etcd;
using Agent.Utils.Misc;
using Newtonsoft.Json;
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

        /*
        Console.WriteLine($"INFO::AgentLifecycleService: Registering to etcd with id {_id}");
		long leaseID = await _etcdClientService.RegisterAgentLeaseAsync(_id, _data);
        Console.WriteLine($"INFO::AgentLifecycleService: Registered to etcd with leaseID: {leaseID}");
        */

        // Assigning to global variables
        Globals.ETCD_ID = _id;
        Globals.ETCD_VALUE = _data;
        Console.WriteLine(_data);
        //Globals.ETCD_LEASE_ID = leaseID;

        // Getting target neighbor
        try
        {
            var nearestNeighbour = await _agnetaClientService.GetAssignedNeighbour();
            ServiceData neighbourData = JsonConvert.DeserializeObject<ServiceData>(nearestNeighbour.Data);

            Console.WriteLine("-----------------------------------------");
            Console.WriteLine(JsonConvert.SerializeObject(neighbourData));
            Console.WriteLine($"nearest neighbour: {neighbourData.Host};");
            Console.WriteLine("-----------------------------------------");
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