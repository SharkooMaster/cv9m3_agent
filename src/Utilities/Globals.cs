
using System.Numerics;
using Agent.Models;
using Models;

namespace Agent.Utils.Globals;


public static class Globals
{
    // ETCD_CONFIG
    public static string ETCD_ID = Misc.Misc.GenerateId();
    public static string ETCD_VALUE = "";
    public static long ETCD_LEASE_ID = -1;

    public static string AgentsLoadbalancer = "http://agent-loadbalancer.default.svc.cluster.local:5000";

    public static int FINGER_TABLE_SIZE = 32;
    // public static M_DHT_Node DHT_NODE = new M_DHT_Node() { Ip = Misc.Misc.GetLocalIPAddress() };
    public static M_Node Self_Node = new M_Node() { node_ip = Misc.Misc.GetLocalIPAddress() };
}