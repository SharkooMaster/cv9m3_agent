
using Agent.Interfaces.Infs;

namespace Agent.Services.Storage
{
    public class NetworkFileStorageService : INetworkFileStorageService
    {
        private readonly string _nfs_path;

        public NetworkFileStorageService(string nfs_path)
        {
            _nfs_path = nfs_path;
        }

        public async Task<(int,int)> StoreVector(string bucket_Id, M_Data data)
        {
            if(string.IsNullOrEmpty(bucket_Id)){ throw new ArgumentNullException(nameof(bucket_Id)); }

            string filePath = Path.Combine(_nfs_path, $"{bucket_Id}.tpdf");
            await File.AppendAllTextAsync(filePath, $"{data.ToJson()}\n");
            return (0,0);
        }

        public async Task<M_Bucket> ReadBucket(string bucket_Id)
        {
            string filePath = Path.Combine(_nfs_path, $"{bucket_Id}.tpdf");
            M_Bucket new_bucket = new M_Bucket(bucket_Id);

            if(!File.Exists(filePath)){ return new_bucket; }

            using(StreamReader reader = new StreamReader(filePath))
            {
                string? line;
                while((line = await reader.ReadLineAsync()) != null)
                {
                    if(!string.IsNullOrWhiteSpace(line))
                    {
                        new_bucket.data.Add(M_Data.FromJson(line));
                    }
                }
            }

            return new_bucket;
        }

        public async Task<byte[]?> GetChunkAsync(string storageGuid)
        {
            if (string.IsNullOrEmpty(storageGuid))
            {
                return null;
            }

            string chunkPath = Path.Combine(_nfs_path, "chunks", storageGuid);
            if (!File.Exists(chunkPath))
            {
                return null;
            }

            return await File.ReadAllBytesAsync(chunkPath);
        }

        public async Task<byte[]?> GetChunkByReferenceAsync(ulong bucketId, ulong bucketIndex)
        {
            // Legacy file backend does not keep a bucket_id/bucket_index -> storage guid index.
            // Return null so caller can handle "not found" gracefully.
            await Task.CompletedTask;
            return null;
        }

        public Task<List<(float[] vector, string storageGuid, long bucketId, long bucketIndex)>> GetVectorsByBucketsAsync(List<string> bucketNames)
            => throw new NotSupportedException("GetVectorsByBucketsAsync not supported on NFS backend.");
    }
}
