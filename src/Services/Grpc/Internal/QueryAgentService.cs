using Agent.Modules.Storage;
using Google.Protobuf.Collections;
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

    public override async Task<QueryResponse> Query(QueryRequest request, ServerCallContext context)
    {
        List<Result> searchResult = await BucketManager.Search(request);

        ResultList resultList = new ResultList();
        resultList.Results.AddRange(searchResult);

        QueryResponse response = new QueryResponse();
        response.ResultLists.Add(resultList);

        return response;
    }
}