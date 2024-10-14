using Agent.Modules.Storage;
using Google.Protobuf.Collections;
using Grpc.Core;
using Grpc.Net.Client;
using TpInternalService;

namespace Agent.Services.Grpc;

public class StoreBucketRowAgentService : StoreBucketRowAgent.StoreBucketRowAgentBase
{
    private readonly ILogger<QueryAgentService> _logger;
    public StoreBucketRowAgentService(ILogger<QueryAgentService> logger)
    {
        _logger = logger;
    }

    public override async Task<StoreResponse> StoreBucketRow(StoreRequest request, ServerCallContext context)
    {
        StoreResponse resp = new StoreResponse();
        resp.Status = true;
        try
        {
            await BucketManager.Store(request);
            return resp;
        }
        catch
        {
            resp.Status = false;
            return resp;
        }
    }
}