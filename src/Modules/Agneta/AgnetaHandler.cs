using Agent.Models.Misc;
using Agent.Modules.Pushover;
using Agent.Services.Agneta;
using Agent.Utils.Globals;
using Newtonsoft.Json;

namespace Agent.Modules.Agneta;

public class LogMessage
{
    [JsonProperty("client_key")]
    public string ClientKey { get; set; }

    [JsonProperty("client_type")]
    public string ClientType { get; set; }

    [JsonProperty("log_level")]
    public int LogLevel { get; set; }

    [JsonProperty("log_message")]
    public string LogMessageText { get; set; }
}


public static class AgnetaHandler
{
    private static AgnetaClientService _instance;

    public static AgnetaClientService Instance => _instance ?? throw new InvalidOperationException("AgnetaClientService not initialized.");

    public static bool disabled = false;

    public static void SetInstance(AgnetaClientService instance)
    {
        _instance = instance ?? throw new ArgumentNullException(nameof(instance));
    }

    public static async Task Log(int _level, string _message)
    {
        if(disabled) { return; }

        if(_instance != null)
        {
            LogMessage _log = new LogMessage();
            _log.ClientKey = Globals._NODE.ip;
            _log.ClientType = "Agent";
            _log.LogLevel = _level;
            _log.LogMessageText = _message;

            await _instance.SendMessageAsync(JsonConvert.SerializeObject(_log));
            //Console.Writeline("Sent a message");
        }
        else
        {
            PushoverHandler.PushNotification($"Gateway:{Globals.ETCD_ID}:Failed to send log to agneta. No service running");
            //Console.Writeline("ERROR::AgnetaHandler.Log: No service running");
        }
    }

    public static async Task SendUsageStats()
    {
        if(disabled) { return; }
        await _instance.SendUsageStatistics();
    }

    public static async Task<NeighbourData> GetNeighbour()
    {
        if(disabled) { return null; }
        return await _instance.GetAssignedNeighbour();
    }

    public static async Task Close()
    {
        if(disabled) { return; }
        await _instance.SendCloseAsync();
    }
}