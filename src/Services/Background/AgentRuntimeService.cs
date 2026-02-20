
using Agent.Utils.Globals;
using Agent.Utils.Misc;
namespace Agent.Services;

/// <summary>
/// Lightweight runtime service. DHT VerifySuccessor/JoinNetwork logic has been removed:
/// - Rendezvous hashing replaced DHT — routing is deterministic, no successor needed
/// - The old VerifySuccessor caused gRPC calls that timed out and wasted thread pool threads
/// - BackgroundServiceManager still runs for any registered routine/fire methods
/// </summary>
public class AgentRuntimeService : BackgroundService
{
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
                await BackgrounfServiceManager.RunRoutineMethods();
                await BackgrounfServiceManager.RunFireMethods();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[AgentRuntimeService TICK ERROR]: {ex.Message}");
            }

            await Task.Delay(TimeSpan.FromSeconds(10), stoppingToken);
        }
    }

    public override async Task StopAsync(CancellationToken stoppingToken)
    {
        Console.WriteLine("INFO::AgentRuntimeService: Agent runtime stopped gracefully.");
        await base.StopAsync(stoppingToken);
    }
}
