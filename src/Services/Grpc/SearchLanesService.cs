using Agent.Services.Cache;
using Agent.Services.Storage;
using Agent.Utils.Globals;
using Google.Protobuf;
using Grpc.Core;

public class SearchLanesService : SearchLanes.SearchLanesBase
{
    public override Task<BatchSearchLanesRes> BatchSearch(BatchSearchLanesReq request, ServerCallContext context)
    {
        var result = new BatchSearchLanesRes();
        if (request.Queries.Count == 0)
            return Task.FromResult(result);

        var bucketStorage = BucketCacheManager.GetBucketStorage();
        if (bucketStorage == null)
            return Task.FromResult(result);

        int maxPerQuery = Globals.LaneSearchMaxPerQuery;

        var queryResults = new LaneQueryResult[request.Queries.Count];
        // Per-query work is a single SearchLaneBucket call — a RocksDB
        // prefix-iterator walk that parks on every block-cache miss. Same
        // handler shape as SearchVector.BatchGet, so use the same cap of
        // ProcessorCount * 4 (≈44 on the L flavor). At the previous *2
        // the lane lookups were leaving most of the CPU idle while the
        // iterators waited on the page cache. .NET ThreadPool already
        // throttles runaway threads so this is a soft hint.
        int maxPar = Math.Max(4, Environment.ProcessorCount * 4);

        Parallel.For(0, request.Queries.Count, new ParallelOptions { MaxDegreeOfParallelism = maxPar }, i =>
        {
            var q = request.Queries[i];
            var qr = new LaneQueryResult { QueryIndex = q.QueryIndex };

            var entries = bucketStorage.SearchLaneBucket((ushort)q.LaneHash, maxPerQuery);

            foreach (var (bucketId, bucketIndex, lanePos, storageGuid, laneBytes) in entries)
            {
                if (laneBytes.Length == 0) continue;

                qr.Matches.Add(new LaneMatch
                {
                    BucketId = bucketId,
                    BucketKey = bucketIndex,
                    StorageGuid = storageGuid,
                    LanePosition = (uint)lanePos,
                    DonorLaneBytes = ByteString.CopyFrom(laneBytes)
                });
            }

            queryResults[i] = qr;
        });

        result.Results.AddRange(queryResults);
        return Task.FromResult(result);
    }
}
