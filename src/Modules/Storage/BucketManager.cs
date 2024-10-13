using System.Collections.Concurrent;
using Agent.Models.Storage;
using TpInternalService;
namespace Agent.Modules.Storage;

public static class BucketManager
{
    public static ConcurrentDictionary<string, Bucket> buckets = new ConcurrentDictionary<string, Bucket>();

    public static async Task<List<TpInternalService.Result>> Search(QueryRequest request)
    {
        List<TpInternalService.Result> results = new List<TpInternalService.Result>();

        if(buckets.ContainsKey(request.Key))
        {
            results.AddRange(await buckets[request.Key].Search(request.Vector.Vec.ToArray(), request.TopK, request.MinThresh));
        }

        return results;
    }
}