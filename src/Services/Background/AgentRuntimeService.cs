
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
                    await AgnetaHandler.Log(1, "Node is null, attempting recovery...");
                    // Add recovery logic here - perhaps re-join the network
                    await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
                    continue;
                }
    
                if (Globals._NODE.successor == null || Globals._NODE.fingerTable == null || Globals._NODE.fingerTable.Count == 0)
                {
                    await AgnetaHandler.Log(1, "Node state is invalid, attempting recovery...");
                    // Add recovery logic here
                    await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
                    continue;
                }
    
                try 
                {
                    Globals._NODE = await NodeService.VerifySuccessor(Globals._NODE);
                }
                catch (Exception ex)
                {
                    await AgnetaHandler.Log(1, $"Error in VerifySuccessor: {ex.Message}");
                }
    
                try 
                {
                    Globals._NODE = await NodeService.FixFingerTable(Globals._NODE);
                }
                catch (Exception ex)
                {
                    await AgnetaHandler.Log(1, $"Error in FixFingerTable: {ex.Message}");
                }
    
                await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
            }
            catch (TaskCanceledException)
            {
                await AgnetaHandler.Log(1, "TaskCanceled, runtime service ending");
                break;
            }
            catch (Exception ex)
            {
                await AgnetaHandler.Log(1, $"Runtime service error: {ex.Message}");
                await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
                // Don't throw here - let the service continue
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