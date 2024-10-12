using Grpc.Core;
using Grpc.Net.Client;
using TpInternalService;

namespace Agent.Services.Grpc;

public class QueryAgentService : QueryAgent.QueryAgentBase
{
    private readonly ILogger<QueryAgentService> _logger;
    public QueryAgentService(ILogger<QueryAgentService> logger)
    {
        _logger = logger;
    }

    public override Task<QueryResponse> Query(QueryRequest request, ServerCallContext context)
    {
        return base.Query(request, context);
    }
}