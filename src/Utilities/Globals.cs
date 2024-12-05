
using System.Numerics;

namespace Agent.Utils.Globals;


public static class Globals
{
    // ETCD_CONFIG
    public static string ETCD_ID = Misc.Misc.GenerateId();
    public static string ETCD_VALUE = "";
    public static long ETCD_LEASE_ID = -1;

    public static string AgentsLoadbalancer = "http://agent-loadbalancer.default.svc.cluster.local:5000";

    public static Vector2 KEY_MIN = new Vector2(0,0);
    public static Vector2 KEY_MAX = new Vector2(1UL << 63, 1UL << 63);
    public static M_DHT_Node DHT_NODE = new M_DHT_Node() { Ip = Misc.Misc.GetLocalIPAddress() };
}