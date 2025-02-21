
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

    public async Task<ulong> InsertData(M_Data _data){
        _data.id = lastId;
        lastId++;
        data.Add(_data);
        Console.WriteLine("Storing on NFS");
        await NetworkFileStorageHandler.StoreVector(ID, _data);
        Console.WriteLine("Done");
        return lastId;
    }
    
    public async Task<List<M_SearchResult>> SearchData(float[] _vector, float _minimum_similarity, int _k)
    {
        List<M_SearchResult> to_return = new List<M_SearchResult>();

        foreach (var row in data)
        {
            if(to_return.Count == _k){ break; }

            float _similarity = Misc.CalculateDistance(_vector, row.vector);
            if(_similarity >= _minimum_similarity)
            {
                to_return.Add(new M_SearchResult() {
                    id = row.id,
                    similarity = _similarity,
                    metadata = row.metadata
                });
            }
        }

        return to_return;
    }

}
