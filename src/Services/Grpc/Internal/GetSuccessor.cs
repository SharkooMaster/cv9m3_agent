
using Agent.Models;
using Agent.Modules.Agneta;
using Agent.Utils.Globals;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.VisualBasic;

public class GetSuccessorService : GetSuccessor.GetSuccessorBase
{
    public override async Task<GetSuccessor_Result> Get(Empty request, ServerCallContext context)
    {
        GetSuccessor_Result res = new GetSuccessor_Result();
        res.Id = Globals._NODE.successor.id;
        res.Ip = Globals._NODE.successor.ip;
        return res;
    }

    public async Task<M_Node> ClientGet(string _ip, CancellationToken ct = default)
    {
        M_Node to_ret = new M_Node();

        var _client = GrpcChannelFactory.GetClient(target: _ip, ctor: chan => new GetSuccessor.GetSuccessorClient(chan), roundRobin: false);

        var deadline = DateTime.UtcNow.AddSeconds(5);
        var response = await _client.GetAsync(new Empty(), deadline: deadline, cancellationToken: ct);

        to_ret.id = response.Id;
        to_ret.ip = response.Ip;

        return to_ret;
    }
}