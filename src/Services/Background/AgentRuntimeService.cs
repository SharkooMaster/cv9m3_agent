
using Agent.Services.Agneta;
using Agent.Services.Etcd;
using Agent.Utils.Misc;
using Agent.Utils.Globals;
using Agent.Interfaces.Agneta;
using Agent.Modules.Agneta;
using Agent.Modules.Peer;
namespace Agent.Services;

public class AgentRuntimeService : BackgroundService
{
    private readonly IEtcdClientService? _etcdClientService;

    public AgentRuntimeService()
    {
        Console.WriteLine("INFO::AgentRuntimeService: Initiating AgentRuntimeService");
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await AgnetaHandler.SendUsageStats();
                Globals._NODE = await NodeService.VerifySuccessor(Globals._NODE);
                Globals._NODE = await NodeService.FixFingerTable(Globals._NODE);
                await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);

                // await NodeService.TestNetwork(Globals._NODE);
            }
            catch (TaskCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                //Console.WriteLine($"Error keeping runtime jobs alive: {ex.Message}");
                await AgnetaHandler.Log(1, $"Error keeping runtime jobs alive: {ex.Message}");
            }
        }

    }

    public override async Task StopAsync(CancellationToken stoppingToken)
    {
        try
        {
            Console.WriteLine("INFO::AgentRuntimeService: Agent runtime stopped gracefully.");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"ERROR::AgentRuntimeService: Error stopping runtime: {ex.Message}");
        }

        await base.StopAsync(stoppingToken);
    }
}