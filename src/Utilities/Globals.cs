
using System.Numerics;

namespace Agent.Utils.Globals;


public static class Globals
{
    // ETCD_CONFIG
    public static string ETCD_ID = Misc.Misc.GenerateId();
    public static string ETCD_VALUE = "";
    public static long ETCD_LEASE_ID = -1;

    public static string AgentsLoadbalancer = "http://agent-loadbalancer.default.svc.cluster.local:5000";

    public static string[] KEYS = new string[2] { "" , "" };
    public static Vector2 KEY_RANGE = new Vector2(0, 10);
}