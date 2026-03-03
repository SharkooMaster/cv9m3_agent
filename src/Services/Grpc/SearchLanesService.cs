
using System.Collections.Concurrent;
using Agent.Modules.Storage;
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

        int subSize = Globals.chunkSize / 64;
        int maxPerQuery = Globals.LaneSearchMaxPerQuery;

        var queryResults = new LaneQueryResult[request.Queries.Count];
        int maxPar = Math.Max(4, Environment.ProcessorCount * 2);

        Parallel.For(0, request.Queries.Count, new ParallelOptions { MaxDegreeOfParallelism = maxPar }, i =>
        {
            var q = request.Queries[i];
            var qr = new LaneQueryResult { QueryIndex = q.QueryIndex };

            var entries = bucketStorage.SearchLaneBucket((ushort)q.LaneHash, maxPerQuery);

            foreach (var (bucketId, bucketIndex, lanePos, storageGuid) in entries)
            {
                if (string.IsNullOrEmpty(storageGuid)) continue;

                // Try to get the chunk bytes from cache to extract the donor lane
                var chunkBytes = ChunkCacheHandler.GetFromCacheOnly(storageGuid);
                if (chunkBytes == null || chunkBytes.Length == 0) continue;

                int donorOffset = lanePos * subSize;
                int donorLen = Math.Min(subSize, chunkBytes.Length - donorOffset);
                if (donorLen <= 0) continue;

                var donorLane = new byte[donorLen];
                Buffer.BlockCopy(chunkBytes, donorOffset, donorLane, 0, donorLen);

                qr.Matches.Add(new LaneMatch
                {
                    BucketId = bucketId,
                    StorageGuid = storageGuid,
                    LanePosition = (uint)lanePos,
                    DonorLaneBytes = ByteString.CopyFrom(donorLane)
                });
            }

            queryResults[i] = qr;
        });

        result.Results.AddRange(queryResults);
        return Task.FromResult(result);
    }
}
