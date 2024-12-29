
using System.Numerics;
using System.Security.Cryptography;
using System.Text;
using Agent.Utils.Misc;
using Google.Protobuf.Reflection;

namespace Agent.Utils;

public static class NodeUtils
{
        public static ulong generateNodeID()
        {
            string ip = Misc.Misc.GetLocalIPAddress();
            string port = "5000";
            // hash using sha256
            // use a hilbert curve or z curve to convert from 2d to 1d
            return 0;
        }
}