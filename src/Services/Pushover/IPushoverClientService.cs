namespace Agent.Services.Pushover;

public interface IPushoverClientService
{
    Task PushNotificationAsync(string message, int _prop = 0);
}