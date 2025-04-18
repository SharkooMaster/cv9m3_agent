
using System.Collections.Concurrent;
using Agent.Utils.Globals;
using Grpc.Net.Client;

public static class GrpcChannelFactory
{
    private static readonly ConcurrentDictionary<string, GrpcChannel> _channels = new ConcurrentDictionary<string, GrpcChannel>();

    public static GrpcChannel GetChannel(string ip)
    {
        return _channels.GetOrAdd(ip, ipAddr => 
        {
            var handler = new SocketsHttpHandler
            {
                EnableMultipleHttp2Connections = true,
                PooledConnectionIdleTimeout = Timeout.InfiniteTimeSpan,
                KeepAlivePingDelay = TimeSpan.FromSeconds(30),
                KeepAlivePingTimeout = TimeSpan.FromSeconds(10)
            };

            Console.WriteLine($"### Connecting to endpoint: {ipAddr}:5000 ###");
            return GrpcChannel.ForAddress($"{ipAddr}:5000", new GrpcChannelOptions
            {
                HttpHandler = handler,
                ServiceConfig = Globals.GRPC_OPTIONS.ServiceConfig,
                LoggerFactory = Globals.GRPC_OPTIONS.LoggerFactory
            });
        });
    }
}