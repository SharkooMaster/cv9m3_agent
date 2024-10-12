
using Agent.Services.Etcd;
using Agent.Utils.Misc;
namespace Agent.Services;

public class AgentRuntimeService : BackgroundService
{
    private readonly IEtcdClientService _etcdClientService;

    public AgentRuntimeService(IEtcdClientService etcdClientService)
    {
        Console.WriteLine("INFO::AgentRuntimeService: Initiating AgentRuntimeService");
        _etcdClientService = etcdClientService;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await _etcdClientService.UpdateHeartBeatAsync(Globals.ETCD_LEASE_ID, stoppingToken);
                
                await Task.Delay(TimeSpan.FromSeconds(8), stoppingToken);
            }
            catch (TaskCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error keeping lease alive: {ex.Message}");
            }
        }

    }

    public override async Task StopAsync(CancellationToken stoppingToken)
    {
        try
        {
            await _etcdClientService.DeregisterAgentLeaseAsync(Globals.ETCD_ID, Globals.ETCD_LEASE_ID);
            Console.WriteLine("INFO::AgentRuntimeService: Agent lease revoked and stopped gracefully.");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"ERROR::AgentRuntimeService: Error revoking lease: {ex.Message}");
        }

        await base.StopAsync(stoppingToken);
    }
}