
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
        Console.WriteLine($"INFO::AgentLifecycleService: Registering to etcd with id {_id}");
		await _etcdClientService.RegisterAgentAsync(_id, Misc.GetServiceInfo("agent", _id));
        Console.WriteLine($"INFO::AgentLifecycleService: Registered to etcd");
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }
}