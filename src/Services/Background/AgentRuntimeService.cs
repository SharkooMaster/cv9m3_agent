
using Agent.Services.Agneta;
using Agent.Services.Etcd;
using Agent.Utils.Misc;
using Agent.Utils.Globals;
using Agent.Interfaces.Agneta;
namespace Agent.Services;

public class AgentRuntimeService : BackgroundService
{
    private readonly IEtcdClientService _etcdClientService;
    private readonly IAgnetaClientService _agnetaClientService;

    public AgentRuntimeService(IAgnetaClientService agnetaClientService)
    {
        Console.WriteLine("INFO::AgentRuntimeService: Initiating AgentRuntimeService");
        _agnetaClientService = agnetaClientService;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await _agnetaClientService.SendUsageStatistics();
                await Task.Delay(TimeSpan.FromSeconds(10), stoppingToken);
            }
            catch (TaskCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error keeping runtime jobs alive: {ex.Message}");
            }
        }

    }

    public override async Task StopAsync(CancellationToken stoppingToken)
    {
        try
        {
            await _etcdClientService.DeregisterAgentLeaseAsync(Globals.ETCD_ID, Globals.ETCD_LEASE_ID);
            Console.WriteLine("INFO::AgentRuntimeService: Agent runtime stopped gracefully.");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"ERROR::AgentRuntimeService: Error stopping runtime: {ex.Message}");
        }

        await base.StopAsync(stoppingToken);
    }
}