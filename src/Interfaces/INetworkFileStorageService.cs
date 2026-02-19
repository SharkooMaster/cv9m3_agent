
using System.Numerics;
using Agent.Models;

namespace Agent.Interfaces.Infs
{
    public interface INetworkFileStorageService
    {
        public Task<(int,int)> StoreVector(string bucket_Id, M_Data data);
        // Store bucket data in files where the name of the file is the 128bit ID.
        public Task<M_Bucket> ReadBucket(string bucket_Id);
        public Task<byte[]?> GetChunkAsync(string storageGuid);
        public Task<byte[]?> GetChunkByReferenceAsync(ulong bucketId, ulong bucketIndex);
        /// <summary>
        /// Batch-fetch vectors from multiple buckets in a single round-trip.
        /// </summary>
        public Task<List<(float[] vector, string storageGuid, long bucketId, long bucketIndex)>> GetVectorsByBucketsAsync(List<string> bucketNames);
    }
}
