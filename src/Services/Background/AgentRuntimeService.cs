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
    private readonly SemaphoreSlim _semaphore = new SemaphoreSlim(1, 1);
    private const int DELAY_SECONDS = 2;

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
                // Ensure only one execution cycle runs at a time
                await _semaphore.WaitAsync(stoppingToken);
                
                try
                {
                    // Execute tasks sequentially
                    await AgnetaHandler.SendUsageStats();
                    
                    if (Globals._NODE != null)
                    {
                        Globals._NODE = await NodeService.VerifySuccessor(Globals._NODE);
                        Globals._NODE = await NodeService.FixFingerTable(Globals._NODE);
                        await AgnetaHandler.Log(1, "fixed fingertable and verified successor");
                    }
                }
                finally
                {
                    _semaphore.Release();
                }

                // Always delay after the work is done, regardless of success or failure
                await Task.Delay(TimeSpan.FromSeconds(DELAY_SECONDS), stoppingToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"ERROR::AgentRuntimeService: Error in runtime jobs: {ex.Message}");
                await AgnetaHandler.Log(1, $"ERROR::AgentRuntimeService: Error in runtime jobs: {ex.Message}");
                // Still delay on error to prevent tight loop
                await Task.Delay(TimeSpan.FromSeconds(DELAY_SECONDS), stoppingToken);
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
        finally
        {
            _semaphore.Dispose();
        }
        
        await base.StopAsync(stoppingToken);
    }
}