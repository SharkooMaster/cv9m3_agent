
using Google.Protobuf;
using System.Text.Json;
using System.Net;
using Agent.Models.Misc;
using System.Numerics;

namespace Agent.Utils.Misc;

public static class Misc
{
    public static string GenerateId()
    {
        return Guid.NewGuid().ToString();
    }

    public static string GetServiceInfo(string service_name, string service_id)
    {
        string _host = GetLocalIPAddress();
        string _port = "5000";
        string _version = "v1.0.0";
        string _env = "dev";

        var serviceInfo = new {
            id          = service_id,
            name        = service_name,
            host        =  _host,
            port        = _port,
            url         = $"http://{_host}:{_port}",
            healthCheck = $"http://{_host}:{_port}/health",
            version     = _version,
            metadata    = new {
                environment = _env,
            }
        };

        return JsonSerializer.Serialize(serviceInfo);
    }

    public static string GetLocalIPAddress()
    {
        string localIP = string.Empty;
        try
        {
            var host = Dns.GetHostEntry(Dns.GetHostName());
            foreach (var ip in host.AddressList)
            {
                if (ip.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork)
                {
                    localIP = ip.ToString();
                    break;
                }
            }
        }
        catch (Exception)
        {
            localIP = "127.0.0.1"; // Fallback to localhost if there's an issue
        }

        return localIP;
    }

    public static double GetMemoryUsagePercentage()
    {
        double totalMemory = 0;
        double freeMemory = 0;

        var lines = File.ReadAllLines("/proc/meminfo");

        foreach (var line in lines)
        {
            if (line.StartsWith("MemTotal:"))
            {
                totalMemory = ParseMemValue(line);
            }
            else if (line.StartsWith("MemAvailable:"))
            {
                freeMemory = ParseMemValue(line);
                break;
            }
        }

        double usedMemory = totalMemory - freeMemory;
        return usedMemory / totalMemory;
    }

    private static double ParseMemValue(string line)
    {
        var parts = line.Split(new[] { ' ', '\t' }, StringSplitOptions.RemoveEmptyEntries);
        return double.Parse(parts[1]) / 1024;
    }

    private static BigInteger ConvertBitStringToBigInteger(string bitString)
    {
        return Convert.ToUInt64(bitString, 2);
    }

    public static bool IsKeyInRange(string startKey, string endKey, string incomingKey)
    {
        BigInteger startValue   = ConvertBitStringToBigInteger(startKey);
        BigInteger endValue     = ConvertBitStringToBigInteger(endKey);
        BigInteger keyValue     = ConvertBitStringToBigInteger(incomingKey);

        if (startValue <= endValue)
        {
            return keyValue >= startValue && keyValue <= endValue;
        }
        else
        {
            return keyValue >= startValue || keyValue <= endValue;
        }
    }

    public static string vector_to_bitstring(float[] _vector)
    {
        string to_return = "";
        for (int i = 0; i < _vector.Length; i++)
        {
            to_return += (_vector[i] >= 0) ? "1" : "0";
        }
        return to_return;
    }

    public static float CalculateDistance(float[] vec1, float[] vec2)
    {
        if (vec1 == null || vec2 == null)
            throw new ArgumentNullException("Vectors must not be null.");

        if (vec1.Length != vec2.Length)
            throw new ArgumentException("Vectors must have the same dimensions.");

        float dotProduct = 0.0f;
        float normVec1 = 0.0f;
        float normVec2 = 0.0f;

        for (int i = 0; i < vec1.Length; i++)
        {
            dotProduct += vec1[i] * vec2[i];
            normVec1 += vec1[i] * vec1[i];
            normVec2 += vec2[i] * vec2[i];
        }

        // Calculate cosine similarity
        float cosineSimilarity = dotProduct / (MathF.Sqrt(normVec1) * MathF.Sqrt(normVec2));

        // Return cosine distance
        return 1 - cosineSimilarity;
    }


}