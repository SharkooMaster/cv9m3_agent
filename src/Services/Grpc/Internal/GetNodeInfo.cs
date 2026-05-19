
using Agent.Utils.Globals;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Grpc.Net.Client;

public class GetNodeInfoService : GetNodeInfo.GetNodeInfoBase
{
    public override async Task<GetNodeInfo_Result> Get(Empty request, ServerCallContext context)
    {
        // ROUTING IDENTITY: Rendezvous Hashing (cross/gateway) keys off this
        // field. Previously we returned MY_NODE_NAME — the Kubernetes node
        // the pod was scheduled to. That meant any pod-to-node reschedule
        // (cordon, NotReady blip, OOMKill, node drain) re-hashed ~1/N
        // buckets to a different agent. With per-agent local storage that
        // silently corrupts decompression: the new agent has different
        // bytes at the same (BucketId, BucketKey) coordinates.
        //
        // MY_POD_NAME is set from `metadata.name` in the manifest. For a
        // StatefulSet the pod name is stable (`crossv9-agent-0`, `-1`, ...)
        // and follows the pod across node reschedules, so routing stays
        // pinned to whichever pod owns the chunks. The Longhorn RWO volume
        // attached via volumeClaimTemplates follows the pod identity, so
        // the chunks remain reachable wherever the pod lands.
        //
        // We keep MY_NODE_NAME as a fallback so a Deployment-mode rollout
        // (no per-pod stable identity) still produces *some* routing key —
        // just one that's only stable as long as the pod isn't rescheduled.
        string routingIdentity =
            Environment.GetEnvironmentVariable("MY_POD_NAME")
            ?? Environment.GetEnvironmentVariable("MY_NODE_NAME")
            ?? "";

        GetNodeInfo_Result res = new GetNodeInfo_Result()
        {
            Ip = Globals._NODE.ip,
            Id = Globals._NODE.id,
            NodeName = routingIdentity
        };
        return res;
    }

    public async Task<GetNodeInfo_Result> ClientGet(string _ip, CancellationToken ct = default)
    {
        try
        {
            var _client = GrpcChannelFactory.GetClient(target: _ip, ctor: chan => new GetNodeInfo.GetNodeInfoClient(chan), roundRobin: false);

            var deadline = DateTime.UtcNow.AddSeconds(Globals.GRPC_TIMEOUT);
            var response = await _client.GetAsync(new Empty(), deadline: deadline, cancellationToken: ct);
            return response;
        }
        catch (RpcException ex)
        {
            Console.WriteLine($"gRPC error: {ex.Status.StatusCode} - {ex.Status.Detail}");
            throw;
        }
        catch(Exception ex)
        {
            Console.WriteLine($"[GetNodeInfo] General error: {ex.Message}");
            throw;
        }
    }
}
