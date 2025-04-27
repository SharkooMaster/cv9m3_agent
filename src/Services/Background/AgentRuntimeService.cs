
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
    private readonly IHostApplicationLifetime _appLifetime;

    public AgentRuntimeService(IHostApplicationLifetime appLifetime)
    {
        Console.WriteLine("INFO::AgentRuntimeService: Initiating AgentRuntimeService");
        _appLifetime = appLifetime;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await Task.Delay(1000);
        _appLifetime.ApplicationStarted.WaitHandle.WaitOne();
        Console.WriteLine("Running fire method");
        await BackgrounfServiceManager.RunFireMethods();

        while (!stoppingToken.IsCancellationRequested)
        {
            if(!AgnetaHandler.disabled)
            {
                await AgnetaHandler.SendUsageStats();
            }

            M_Node temp = await NodeService.VerifySuccessor(Globals._NODE);
            Globals._NODE = temp;

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