
using Agent.Services.Etcd;
namespace Agent.Services;

public class AgentLifeCycleService : IHostedService
{
    private readonly IEtcdClientService _etcdClientService;
    public AgentLifeCycleService(IEtcdClientService etcdClientService)
    {
        _etcdClientService = etcdClientService;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        await _etcdClientService.RegisterAgentAsync("agent-id", "");
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }

}