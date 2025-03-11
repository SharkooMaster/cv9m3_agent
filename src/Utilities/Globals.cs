
using System.Numerics;
using Agent.Models;
using Grpc.Net.Client;
// using Models;

namespace Agent.Utils.Globals;


public static class Globals
{
    // ETCD_CONFIG
    public static string ETCD_ID = Misc.Misc.GenerateId();
    public static string ETCD_VALUE = "";
    public static long ETCD_LEASE_ID = -1;

    public static string AgentsLoadbalancer = "http://192.168.50.243:80";

    public static GrpcChannelOptions GRPC_OPTIONS = new GrpcChannelOptions{
        MaxReceiveMessageSize = 1000*1024*1024,
        MaxSendMessageSize = 1000*1024*1024
    };

    public static int RPU_SECTION = 0;
    public static int RPU_SECTION_MAX = 3;
    public static int FINGER_TABLE_SIZE = 63;
    public static int SUCCESSOR_LIST_SIZE = 4;
    public static M_Node _NODE = new M_Node();
}