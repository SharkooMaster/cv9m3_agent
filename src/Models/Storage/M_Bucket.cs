
using System.Collections.Concurrent;
using System.Numerics;
using Agent.Utils;
using Agent.Utils.Misc;

public class M_Bucket
{
    public string ID { get; set; }
    public ulong lastId = 0; // Possibly needs atomic operations to avoid collision
    public ConcurrentBag<M_Data> data = new ConcurrentBag<M_Data>();

    public M_Bucket(string _ID)
    {
        ID = _ID;
    }

    public async Task<ulong> BookId()
    {
        ulong to_return = lastId;
        Interlocked.Increment(ref lastId);
        return to_return;
    }

    public async Task<(ulong, ulong)> InsertData(M_Data _data, ulong _id){
        // Console.Writeline("Storing on NFS");
        var toRet = ((ulong,ulong))(await NetworkFileStorageHandler.StoreVector(ID, _data));
        _data.id = toRet.Item1;
        _data.index = toRet.Item2;
        // Console.Writeline("Done");
        return toRet;
    }
    
    public async Task<List<M_SearchResult>> SearchData(float[] _vector, float _minimum_similarity, int _k)
    {
        ConcurrentBag<M_SearchResult> to_return = new ConcurrentBag<M_SearchResult>();

        Parallel.ForEach(data, row => {
            float _similarity = Misc.CalculateDistance(_vector, row.vector);
            
            if(row.chunk != null || row.chunk.Length != 0)
            {
              Console.WriteLine("### ERROR ###} Chunk has no value");
            }

            if(_similarity >= _minimum_similarity && to_return.Count != _k)
            {
                to_return.Add(new M_SearchResult() {
                    id = row.id,
                    index = row.index,
                    similarity = _similarity,
                    chunk = row.chunk,
                });
            }
        });

        return to_return.ToList();
    }

}
