
using System.Numerics;
using Agent.Models;
using Grpc.Net.Client;
using Grpc.Net.Client.Configuration;
// using Models;
using System;

namespace Agent.Utils.Globals;


public static class Globals
{
    // ETCD_CONFIG
    public static string ETCD_ID = Misc.Misc.GenerateId();
    public static string ETCD_VALUE = "";
    public static long ETCD_LEASE_ID = -1;

    public static int chunkSize = int.TryParse(Environment.GetEnvironmentVariable("CHUNK_SIZE"), out var cs) ? cs : 5120;

    // Allow running outside Kubernetes/Docker by overriding via env var.
    // Examples:
    // - AGENTS_LOADBALANCER=localhost
    // - AGENTS_LOADBALANCER=127.0.0.1
    // - AGENTS_LOADBALANCER=agent-1 (docker-compose / k8s service)
    public static string AgentsLoadbalancer = Environment.GetEnvironmentVariable("AGENTS_LOADBALANCER") ?? "agent-1";
    public static string? bootstrap_node = null;
    public static bool bootstraped = false;

    /// <summary>
    /// Set to true ONLY after WarmUpBuckets() completes.
    /// The readiness probe returns 503 until this is true,
    /// preventing K8s from routing traffic to an unready agent.
    /// </summary>
    public static volatile bool IsReady = false;

    // LOCAL MODE: Reduce retries in local mode (local network is reliable)
    // Note: gRPC requires MaxAttempts > 1, so use 2 for local mode
    private static int GetMaxRetryAttempts()
    {
        return LocalModeDetector.IsLocalMode() ? 2 : 4;
    }

    public static GrpcChannelOptions GRPC_OPTIONS = new GrpcChannelOptions
    {
        HttpHandler = new SocketsHttpHandler()
        {
            EnableMultipleHttp2Connections = true,
            PooledConnectionIdleTimeout = Timeout.InfiniteTimeSpan,
            KeepAlivePingDelay = TimeSpan.FromSeconds(30),
            KeepAlivePingTimeout = TimeSpan.FromSeconds(10)
        },
        // Debug logging removed — it serialises all gRPC output through
        // Console.WriteLine under the hood, creating a global lock that
        // blocks every concurrent handler when there are 100+ in-flight RPCs.
        ServiceConfig = new Grpc.Net.Client.Configuration.ServiceConfig()
        {
            MethodConfigs =
            {
                new Grpc.Net.Client.Configuration.MethodConfig
                {
                    Names = { MethodName.Default },
                    RetryPolicy = new RetryPolicy
                    {
                        MaxAttempts = GetMaxRetryAttempts(),
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
    public static int GRPC_TIMEOUT = 20;
    public static M_Node _NODE = new M_Node();
}