
using System.Data;
using System.Text.Json;
using Agent.Utils.Misc;
using Google.Protobuf;

namespace Agent.Services.Agneta
{
    public class NeighbourData
    {
        public string NodeType { get; set; }
        public double LoadScore { get; set; }
        public int Id { get; set; }
        public JsonElement Data { get; set; }
    }
    public class AgnetaClientService : IAgnetaClientService
    {
        private readonly HttpClient _client;
        private readonly string _url;

        public AgnetaClientService()
        {
            _client = new HttpClient();
            _url    = "https://agneta-loadbalancer.default.svc.cluster.local:443";
        }

        public async Task<NeighbourData> GetAssignedNeighbour()
        {
            try
            {
                Console.WriteLine("Contacting Agneta: Assign neighbour");
                var response = await _client.GetFromJsonAsync<NeighbourData>($"{_url}/lbs/agent/pop");
                Console.WriteLine("Contacting Agneta returned successful");
                return response;
            }
            catch
            {
                Console.WriteLine("ERROR::AgnetaClientService: Failed to get assigned neighbour");
            }
            return null;
        }
    }
}