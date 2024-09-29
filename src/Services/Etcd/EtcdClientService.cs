
using System.Data;
using dotnet_etcd;
using Etcdserverpb;
using Google.Protobuf;

namespace Agent.Services.Etcd
{
    public class EtcdClientService : IEtcdClientService
    {
        private readonly EtcdClient _etcdClient;

        public EtcdClientService(EtcdClient etcdClient)
        {
            _etcdClient = etcdClient;
        }

        public async Task<long> RegisterAgentLeaseAsync(string agentId, string data)
        {
            var leaseGrantResponse = await _etcdClient.LeaseGrantAsync(new LeaseGrantRequest{ TTL = 10 });
            var leaseID = leaseGrantResponse.ID;

            await _etcdClient.PutAsync(new PutRequest{
                Key = ByteString.CopyFromUtf8($"/agents/{agentId}"),
                Value = ByteString.CopyFromUtf8(data),
                Lease = leaseID
            });

            return leaseID;
        }

        public async Task RegisterAgentAsync(string agentId, string data)
        {
            await _etcdClient.PutAsync($"/agents/{agentId}", data);
        }

        public async Task UpdateHeartBeatAsync(long leaseId, CancellationToken stoppingToken)
        {
            await _etcdClient.LeaseKeepAlive(leaseId, stoppingToken);
        }

        public async Task DeregisterAgentLeaseAsync(string agentId, long leaseId)
        {
            await _etcdClient.LeaseRevokeAsync(new LeaseRevokeRequest{ ID = leaseId });
            await _etcdClient.DeleteAsync($"/agents/{agentId}");
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