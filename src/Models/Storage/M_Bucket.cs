
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

    public async Task InsertData(M_Data _data, ulong _id){
        _data.id = _id;
        data.Add(_data);
        //Console.Writeline("Storing on NFS");
        await NetworkFileStorageHandler.StoreVector(ID, _data);
        //Console.Writeline("Done");
    }
    
    public async Task<List<M_SearchResult>> SearchData(float[] _vector, float _minimum_similarity, int _k)
    {
        List<M_SearchResult> to_return = new List<M_SearchResult>();

        Parallel.ForEach(data, row => {
            float _similarity = Misc.CalculateDistance(_vector, row.vector);
            //Console.Writeline($"sim: {_similarity}, minSim: {_minimum_similarity}");
            if(_similarity >= _minimum_similarity)
            {
                to_return.Add(new M_SearchResult() {
                    id = row.id,
                    similarity = _similarity,
                    metadata = row.metadata
                });
            }
        });

        return to_return;
    }

}
