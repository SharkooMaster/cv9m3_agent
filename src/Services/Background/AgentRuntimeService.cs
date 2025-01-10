
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

                if (Globals._NODE == null)
                {
                    throw new NullReferenceException("Globals._NODE is null");
                }

                Globals._NODE = await NodeService.VerifySuccessor(Globals._NODE);
                Globals._NODE = await NodeService.FixFingerTable(Globals._NODE);
                await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);

                // await NodeService.TestNetwork(Globals._NODE);
            }
            catch (TaskCanceledException)
            {
                await AgnetaHandler.Log(1, $"TaskCanceled, runtime service ending");
                break;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"ERROR::AgentRuntimeService: {ex.Message}");
                if (Globals._NODE == null)
                {
                    Console.WriteLine("ERROR::AgentRuntimeService: Globals._NODE is null.");
                }
                await AgnetaHandler.Log(1, $"ERROR::AgentRuntimeService: {ex}");
                throw;
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