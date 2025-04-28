
using Agent.Services.Agneta;
using Agent.Services.Etcd;
using Agent.Utils.Misc;
using Agent.Utils.Globals;
using Agent.Interfaces.Agneta;
using Agent.Modules.Agneta;
using Agent.Modules.Peer;
using Agent.Models;
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
        // !stoppingToken.IsCancellationRequested
        while (true)
        {
            try
            {
                Console.WriteLine("runtime tick");

                if (!AgnetaHandler.disabled)
                {
                    await AgnetaHandler.SendUsageStats();
                }

                if (Globals._NODE != null && Globals._NODE.successor != null && Globals.bootstraped)
                {
                    try
                    {
                        M_Node temp = await NodeService.VerifySuccessor(Globals._NODE);
                        Globals._NODE = temp;
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[RUNTIME ERROR]: VerifySuccessor failed: {ex.Message}");
                    }
                }
                else
                {
                    Console.WriteLine("Waiting for node to join network...");
                }

                // await BackgrounfServiceManager.RunRoutineMethods();
                // await BackgrounfServiceManager.RunFireMethods();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[AgentRuntimeService TICK ERROR]: {ex}");
            }

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