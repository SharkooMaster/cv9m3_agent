
using System.Numerics;
using Agent.Models;

namespace Agent.Interfaces.Infs
{
    public interface INetworkFileStorageService
    {
        public Task<(ulong,ulong)> StoreVector(string bucket_Id, M_Data data);
        // Store bucket data in files where the name of the file is the 128bit ID.
        public Task<M_Bucket> ReadBucket(string bucket_Id);
        public Task<byte[]?> GetChunkAsync(string storageGuid);
        public Task<byte[]?> GetChunkByReferenceAsync(ulong bucketId, ulong bucketIndex);
        /// <summary>
        /// Batch-fetch vectors from multiple buckets in a single round-trip.
        /// Returns (vector, storageGuid, bucketId, bucketIndex, bucketName).
        /// </summary>
        public Task<List<(float[] vector, string storageGuid, long bucketId, long bucketIndex, string bucketName)>> GetVectorsByBucketsAsync(List<string> bucketNames);

        /// <summary>
        /// Flush all pending writes to durable storage (SSD/disk).
        /// Must be called before responding to RPCs to guarantee crash safety.
        /// After this returns, all data is persisted even if the process is killed.
        /// </summary>
        void FlushPendingWrites();
    }
}
