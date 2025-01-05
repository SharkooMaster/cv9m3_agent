
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Grpc.Net.Client;

public class GetHealthService : GetHealth.GetHealthBase
{
    public override async Task<GetHealth_Result> Get(Empty request, ServerCallContext context)
    {
        return new GetHealth_Result() { Status = "Healthy" };
    }

    public async Task<GetHealth_Result> ClientGet(string _ip)
    {
        var channel = GrpcChannel.ForAddress($"http://{_ip}:5000");
        GetHealth.GetHealthClient _client = new GetHealth.GetHealthClient(channel);

        var result = await _client.GetAsync(new Empty());
        return result;
    }
}
