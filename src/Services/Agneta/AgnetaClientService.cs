
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
            _url    = "http://192.168.49.2:30209";
        }

        public async Task GetAssignedNeighbour()
        {
            var response = await _client.GetFromJsonAsync<NeighbourData>($"{_url}/lbs/agent/pop");
        }
    }
}