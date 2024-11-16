
namespace Agent.Utils.Globals;

public static class Globals
{
    // ETCD_CONFIG
    public static string ETCD_ID = "";
    public static string ETCD_VALUE = "";
    public static long ETCD_LEASE_ID = -1;

    public static string AgentsLoadbalancer = "http://agent-loadbalancer.default.svc.cluster.local:5000";
}