
using System.Numerics;
using Agent.Modules.Agneta;
using TpInternalService;

namespace Agent.Models;

public class M_Node
{
    /*
        Node ID generation and management
        Successor/predecessor handling
        Finger table implementation
        Join/leave protocols
        Basic routing
    */
    public int node_id {get;set;}
    public M_Node? successor {get;set;}
    public M_Node? predeccessor {get;set;}
    public Dictionary<int, M_Node> finger_table = new Dictionary<int, M_Node>();
    public M_VectorStore? vector_store { get; set; }

    public async Task<QueryResponse> find_similarity(float[] _vector)
    {
        if(vector_store == null)
        {
            await AgnetaHandler.Log(2, "No vector_store created for M_Node");
            return new QueryResponse();
        }
        (List<M_SearchResult> search_results, int search_table, Vector2 bucket_coordinates) = await vector_store.find_similar(_vector);

        if(search_table == 1)
        {
        }
        return new QueryResponse();
    }
}
