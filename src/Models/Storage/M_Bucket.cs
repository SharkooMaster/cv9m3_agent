
using System.Collections.Concurrent;
using Agent.Utils;
using Agent.Utils.Misc;

public class M_Bucket
{
    public ConcurrentBag<M_Data> data = new ConcurrentBag<M_Data>();

    public async Task InsertData(M_Data _data){ data.Add(_data); }
    
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
