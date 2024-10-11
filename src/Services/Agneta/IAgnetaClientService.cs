
namespace Agent.Services.Agneta
{
    public interface IAgnetaClientService
    {
        Task GetAssignedNeighbour();
        //Task RegisterAgentAsync(String agentId, string data);
        //Task<long> RegisterAgentLeaseAsync(String agentId, string data);
        //Task UpdateHeartBeatAsync(long leaseId, CancellationToken stoppingToken);
        //Task DeregisterAgentLeaseAsync(string agentId, long leaseId);
        //Task DeregisterAgentAsync(string agentId);
        //Task<string> GetAgentStatusAsync(string agentId);
    }
}