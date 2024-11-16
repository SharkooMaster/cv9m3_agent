using Agent.Services.PushOver;

namespace Agent.Modules.Pushover;

public static class PushoverHandler
{
    private static PushoverClientService _instance;

    public static PushoverClientService Instance => _instance ?? throw new InvalidOperationException("PushoverClientService not initialized.");

    public static void SetInstance(PushoverClientService instance)
    {
        _instance = instance ?? throw new ArgumentNullException(nameof(instance));
    }

    public static async void PushNotification(string message)
    {
        await _instance.PushNotificationAsync(message);
    }
}