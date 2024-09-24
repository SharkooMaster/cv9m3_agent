
using Agent.Services.Etcd;
using Agent.Utils.Misc;
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
        await _etcdClientService.RegisterAgentAsync(Misc.GenerateId(), "");
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }
}