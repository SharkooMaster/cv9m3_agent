using Agent.Services.Pushover;

namespace Agent.Services.PushOver;

public class PushoverClientService : IPushoverClientService
{
    private static readonly HttpClient client = new HttpClient();

    private string _token {get;set;}
    private string _user_key {get;set;}
    public PushoverClientService()
    {
        _token    = Environment.GetEnvironmentVariable("PUSHOVER_TOKEN");
        _user_key = Environment.GetEnvironmentVariable("PUSHOVER_USER_KEY");

        if(_token == "" || _user_key == "")
        {
            Console.WriteLine("ERROR::PushoverClientServices:Failed to retrieve pushover credentials from environment");
        }
    }

    public async Task PushNotificationAsync(string message, int _prio = 0)
    {
        var values = new Dictionary<string, string>
        {
            { "token", _token },
            { "user", _user_key },
            { "message", message },
            { "priority", _prio.ToString()}
        };

        var content = new FormUrlEncodedContent(values);
        var response = await client.PostAsync("https://api.pushover.net/1/messages.json", content);

        if(!response.IsSuccessStatusCode)
        {
            Console.WriteLine($"ERROR::PushoverClientServices: Could not push notification => {response.Content.ReadAsStringAsync().Result}");
            Console.WriteLine($"token: {_token}");
            Console.WriteLine($"userKey: {_user_key}");
        }
    }
}