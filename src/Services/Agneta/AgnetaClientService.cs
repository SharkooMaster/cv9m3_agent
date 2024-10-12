
using System.Data;
using System.Text.Json;
using Agent.Utils.Misc;
using Google.Protobuf;
using Newtonsoft.Json;

namespace Agent.Services.Agneta
{
    public class NeighbourData
    {
        [JsonProperty("node_type")]
        public string NodeType { get; set; }

        [JsonProperty("load_score")]
        public double LoadScore { get; set; }
        public int Id { get; set; }
        public string Data { get; set; }
    }
    public class AgnetaClientService : IAgnetaClientService
    {
        private readonly HttpClient _client;
        private readonly string _url;

        public AgnetaClientService()
        {
            var handler = new HttpClientHandler(){
                ServerCertificateCustomValidationCallback = HttpClientHandler.DangerousAcceptAnyServerCertificateValidator
            };

            _client = new HttpClient(handler);
            _url    = "https://agneta-loadbalancer.default.svc.cluster.local:443";
        }

        public async Task<NeighbourData> GetAssignedNeighbour()
        {
            try
            {
                Console.WriteLine("Contacting Agneta: Assign neighbour");
                var response = await _client.GetAsync($"{_url}/lbs/agent/pop");

                if (response.IsSuccessStatusCode)
                {
                    var jsonResponse = await response.Content.ReadAsStringAsync();
                    Console.WriteLine($"Contacting Agneta returned successful: {jsonResponse}");

                    if(jsonResponse != null)
                    {
                        // Deserialize the JSON response
                        NeighbourData toReturn = JsonConvert.DeserializeObject<NeighbourData>(jsonResponse);
                        return toReturn;
                    }
                    else
                    {
                        Console.WriteLine("ERROR::AgnetaClientService: Failed to ge assigned neighbour. Response not a valid json. Standby");
                        return null;
                    }
                }
                else
                {
                    Console.WriteLine($"ERROR::AgnetaClientService: Failed to get assigned neighbour. Status Code: {response.StatusCode}");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"ERROR::AgnetaClientService: Failed to get assigned neighbour. Exception: {ex.Message}");
            }

            return null;
        }

    }
}