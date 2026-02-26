using Agent.Interfaces.Infs;
using Agent.Services.Storage;
using Agent.Utils.Globals;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;

namespace Agent.Services.Grpc;

public class StorageStatsService : StorageStats.StorageStatsBase
{
    private readonly INetworkFileStorageService _storage;

    public StorageStatsService(INetworkFileStorageService storage)
    {
        _storage = storage;
    }

    public override Task<StorageStats_Result> GetStats(Empty request, ServerCallContext context)
    {
        long uniqueChunks = 0;
        long totalBytes = 0;
        long totalBuckets = 0;
        long totalVectors = 0;

        if (_storage is RocksDbStorageService rocksDb)
        {
            (uniqueChunks, totalBytes) = rocksDb.GetChunkStorageStats();
            (totalBuckets, totalVectors) = rocksDb.BucketStorage.GetBucketAndVectorStats();
        }

        return Task.FromResult(new StorageStats_Result
        {
            TotalUniqueChunks = (ulong)uniqueChunks,
            TotalChunkBytes = (ulong)totalBytes,
            TotalBuckets = (ulong)totalBuckets,
            TotalVectors = (ulong)totalVectors,
            NodeName = Environment.GetEnvironmentVariable("MY_NODE_NAME") ?? ""
        });
    }
}
