
using Agent.Utils.Globals;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Grpc.Net.Client;

public class GetHealthService : GetHealth.GetHealthBase
{
    public override async Task<GetHealth_Result> Get(Empty request, ServerCallContext context)
    {
        return new GetHealth_Result() { Status = "Healthy" };
    }

    public async Task<GetHealth_Result> ClientGet(string _ip, CancellationToken ct = default)
    {
        var channel = GrpcChannelFactory.GetChannel(_ip);
        GetHealth.GetHealthClient _client = new GetHealth.GetHealthClient(channel);

        var deadline = DateTime.UtcNow.AddSeconds(5);
        var result = await _client.GetAsync(new Empty(), deadline: deadline, cancellationToken: ct);
        return result;
    }
}
