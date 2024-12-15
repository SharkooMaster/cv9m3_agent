using System.Collections.Concurrent;
using Agent.Models.Storage;
using Google.Protobuf;
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

    public static async Task Store(StoreRequest request)
    {
        await Task.Run(()=>{
            if(!buckets.ContainsKey(request.Key))
            {
                Bucket newBucket = new Bucket(){ bucketKey = request.Key };
                bool isCreated = buckets.TryAdd(request.Key, newBucket);

                if(!isCreated)
                {
                    Console.WriteLine("ERROR::BucketManager: Store() -> Failed to create a new bucket.");
                    return;
                }
            }

            buckets[request.Key].bucketRows.Add(new BucketRow(){
                id = request.Id,
                chunk = request.Chunk.ToArray(),
                vector = request.Vector.Vec.ToArray()
            });
        });
    }
}