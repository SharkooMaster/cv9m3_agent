
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

        string input = $"{ip}:{port}";

        using(SHA256 sha256 = SHA256.Create())
        {
            byte[] hashBytes = sha256.ComputeHash(Encoding.UTF8.GetBytes(input));

            // XOR the hash bytes into a single 64-bit integer
            ulong hash64 = 0;
            for (int i = 0; i < hashBytes.Length; i++)
            {
                ulong segment = BitConverter.ToUInt64(hashBytes, i % (hashBytes.Length - 7));
                hash64 ^= segment;
            }

            return hash64;
        }
    }

    public static bool inBetween(ulong id, ulong start, ulong end)
    {
        if(start < end)
        {
            return start < id && id <= end;
        }
        else
        {
            return id > start || id <= end;
        }
    }
}