
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

        public async Task StoreVector(string bucket_Id, M_Data data)
        {
            if(string.IsNullOrEmpty(bucket_Id)){ throw new ArgumentNullException(nameof(bucket_Id)); }

            // string filePath = Path.Combine(_nfs_path, $"{bucket_Id}.tpdf");
            string filePath = $"{bucket_Id}.tpdf";
            await File.AppendAllTextAsync(filePath, $"{data.ToJson()}\n");
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
    }
}
