
using System.Numerics;
using Agent.Models;
using Grpc.Net.Client;
using Grpc.Net.Client.Configuration;
// using Models;

namespace Agent.Utils.Globals;


public static class Globals
{
    // ETCD_CONFIG
    public static string ETCD_ID = Misc.Misc.GenerateId();
    public static string ETCD_VALUE = "";
    public static long ETCD_LEASE_ID = -1;

    public static string AgentsLoadbalancer = "http://agent-loadbalancer.default.svc.cluster.local:80";

    public static GrpcChannelOptions GRPC_OPTIONS = new GrpcChannelOptions
    {
        HttpHandler = new SocketsHttpHandler()
        {
            EnableMultipleHttp2Connections = true,
            PooledConnectionIdleTimeout = Timeout.InfiniteTimeSpan,
            KeepAlivePingDelay = TimeSpan.FromSeconds(30),
            KeepAlivePingTimeout = TimeSpan.FromSeconds(10)
        },
        LoggerFactory = LoggerFactory.Create(lb =>
        {
            lb.AddConsole();
            lb.SetMinimumLevel(LogLevel.Debug);
        }),
        ServiceConfig = new Grpc.Net.Client.Configuration.ServiceConfig()
        {
            MethodConfigs =
            {
                new Grpc.Net.Client.Configuration.MethodConfig
                {
                    Names = { MethodName.Default },
                    RetryPolicy = new RetryPolicy
                    {
                        MaxAttempts = 4,
                        InitialBackoff = TimeSpan.FromMilliseconds(100),
                        MaxBackoff = TimeSpan.FromSeconds(1),
                        BackoffMultiplier = 2,
                        RetryableStatusCodes = { Grpc.Core.StatusCode.Unavailable, Grpc.Core.StatusCode.ResourceExhausted}
                    }
                }
            }
        },
        MaxReceiveMessageSize = 1000 * 1024 * 1024,
        MaxSendMessageSize = 1000 * 1024 * 1024
    };

    public static int RPU_SECTION = 0;
    public static int RPU_SECTION_MAX = 3;
    public static int FINGER_TABLE_SIZE = 63;
    public static int SUCCESSOR_LIST_SIZE = 4;
    public static M_Node _NODE = new M_Node();
}