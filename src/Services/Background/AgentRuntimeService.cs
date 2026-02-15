
using Agent.Services.Agneta;
// using Agent.Services.Etcd; // REMOVED: No longer using etcd
using Agent.Utils.Misc;
using Agent.Utils.Globals;
using Agent.Interfaces.Agneta;
using Agent.Modules.Agneta;
using Agent.Modules.Peer;
using Agent.Models;
namespace Agent.Services;

public class AgentRuntimeService : BackgroundService
{
    // REMOVED: No longer using etcd, using Kubernetes service discovery instead
    // private readonly IEtcdClientService? _etcdClientService;

    public AgentRuntimeService()
    {
        Console.WriteLine("INFO::AgentRuntimeService: Initiating AgentRuntimeService");
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        int consecutiveFailures = 0;

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                Console.WriteLine("runtime tick");

                if (Globals._NODE != null && Globals._NODE.successor != null && Globals.bootstraped)
                {
                    try
                    {
                        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
                        M_Node temp = await NodeService.VerifySuccessor(Globals._NODE);
                        Globals._NODE = temp;
                        consecutiveFailures = 0;
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[RUNTIME ERROR]: VerifySuccessor failed: {ex.Message}");
                        consecutiveFailures++;

                        // After multiple consecutive failures, try to reconnect
                        if (consecutiveFailures > 5)
                        {
                            Console.WriteLine($"[RUNTIME WARNING]: Too many consecutive failures ({consecutiveFailures}). Attempting to rejoin network...");
                            try
                            {
                                // Attempt to find a new successor
                                Globals._NODE = await NodeService.JoinNetwork(Globals._NODE, Globals.bootstrap_node);
                                consecutiveFailures = 0;
                            }
                            catch (Exception rejoinEx)
                            {
                                Console.WriteLine($"[RUNTIME ERROR]: Failed to rejoin network: {rejoinEx.Message}");
                            }
                        }
                    }
                }
                else
                {
                    Console.WriteLine("Waiting for node to join network...");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[AgentRuntimeService TICK ERROR]: {ex}");
            }

            await BackgrounfServiceManager.RunRoutineMethods();
            await BackgrounfServiceManager.RunFireMethods();

            await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
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