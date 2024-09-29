
namespace Agent.Services.Etcd
{
    public interface IEtcdClientService
    {
        Task RegisterAgentAsync(String agentId, string data);
        Task UpdateHeartBeatAsync(string agentId);
        Task DeregisterAgentAsync(string agentId);
        Task<string> GetAgentStatusAsync(string agentId);
    }
}