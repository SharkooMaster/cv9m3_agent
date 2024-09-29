
using System.Data;
using dotnet_etcd;

namespace Agent.Services.Etcd
{
    public class EtcdClientService : IEtcdClientService
    {
        private readonly EtcdClient _etcdClient;

        public EtcdClientService(EtcdClient etcdClient)
        {
            _etcdClient = etcdClient;
        }

        public async Task RegisterAgentAsync(string agentId, string data)
        {
            await _etcdClient.PutAsync($"/agents/{agentId}", data);
        }

        public async Task UpdateHeartBeatAsync(string agentId)
        {
            await _etcdClient.PutAsync($"/agents/{agentId}/heartbeat", DateTime.UtcNow.ToString());
        }

        public async Task DeregisterAgentAsync(string agentId)
        {
            await _etcdClient.DeleteAsync($"/agents/{agentId}");
        }

        public async Task<string> GetAgentStatusAsync(string agentId)
        {
            var response = await _etcdClient.GetAsync($"/agents/{agentId}");

            // Ensure response contains key-value pair.
            if(response.Kvs.Count > 0)
            {
                return response.Kvs[0].Value.ToStringUtf8();
            }

            return null; // If no key-value pair found.
        }
    }
}