
using Agent.Services.Etcd;
using Agent.Utils.Misc;
namespace Agent.Services;

public class AgentLifeCycleService : IHostedService
{
    private readonly IEtcdClientService _etcdClientService;

    public AgentLifeCycleService(IEtcdClientService etcdClientService)
    {
        Console.WriteLine("INFO::AgentLifecycleService: Initiating AgentLifeCycleService");
        _etcdClientService = etcdClientService;
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
        //Globals.ETCD_LEASE_ID = leaseID;

        // Getting target neighbor
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }
}