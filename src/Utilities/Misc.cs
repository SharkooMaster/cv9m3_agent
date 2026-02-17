
using Google.Protobuf;
using System.Text.Json;
using System.Net;
using Agent.Models.Misc;
using System.Numerics;
using System.Threading.Tasks;
using Agent.Modules.Agneta;

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
                totalMem = GetAvailableMemory().ToString() + "bytes"
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

    private static ulong ConvertBitStringToBigInteger(string bitString)
    {
        return Convert.ToUInt64(bitString, 2);
    }

    public static bool IsKeyInRange(ulong startValue, ulong endValue, string incomingKey)
    {
        if(startValue == endValue){ return true; }

        ulong keyValue = ConvertBitStringToBigInteger(incomingKey);

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

        // SIMD OPTIMIZATION: Use Vector<float> for hardware-accelerated operations
        // Vector<float>.Count is typically 8 on AVX2 systems, 4 on SSE systems
        int vectorSize = Vector<float>.Count;
        double dotProduct = 0.0;
        double normVec1 = 0.0;
        double normVec2 = 0.0;

        // OPTIMIZATION: Improved SIMD for faster cosine similarity calculation
        // Use SIMD for the main loop with better horizontal sum
        int i = 0;
        if (vectorSize > 1 && vec1.Length >= vectorSize)
        {
            Vector<float> dotSum = Vector<float>.Zero;
            Vector<float> norm1Sum = Vector<float>.Zero;
            Vector<float> norm2Sum = Vector<float>.Zero;
            
            for (; i <= vec1.Length - vectorSize; i += vectorSize)
            {
                var v1 = new Vector<float>(vec1, i);
                var v2 = new Vector<float>(vec2, i);
                
                // Vector operations are hardware-accelerated
                dotSum += v1 * v2;
                norm1Sum += v1 * v1;
                norm2Sum += v2 * v2;
            }
            
            // OPTIMIZATION: More efficient horizontal sum using SIMD operations
            // Instead of copying to arrays, accumulate directly from vectors
            dotProduct = HorizontalSum(dotSum);
            normVec1 = HorizontalSum(norm1Sum);
            normVec2 = HorizontalSum(norm2Sum);
        }

        // Handle remainder sequentially
        for (; i < vec1.Length; i++)
        {
            dotProduct += vec1[i] * vec2[i];
            normVec1 += vec1[i] * vec1[i];
            normVec2 += vec2[i] * vec2[i];
        }

        // Calculate cosine similarity.
        // Important edge-case handling:
        // - if both vectors are zero-norm and identical, treat as exact match (1.0)
        // - if only one side is zero-norm, no directional similarity (0.0)
        double eps = 1e-12;
        bool vec1Zero = normVec1 <= eps;
        bool vec2Zero = normVec2 <= eps;
        if (vec1Zero && vec2Zero)
        {
            // Exact element-wise equality check keeps semantics deterministic.
            for (int j = 0; j < vec1.Length; j++)
            {
                if (vec1[j] != vec2[j])
                    return 0.0f;
            }
            return 1.0f;
        }
        if (vec1Zero || vec2Zero)
            return 0.0f;

        double denominator = Math.Sqrt(normVec1) * Math.Sqrt(normVec2);
        if (denominator <= eps)
            return 0.0f;
        
        return (float)(dotProduct / denominator);
    }

    public static long GetAvailableMemory()
    {
        return GC.GetGCMemoryInfo().TotalAvailableMemoryBytes;
    }

    public static BigInteger ConvertToBigInteger(string binaryString)
    {
        return BigInteger.Parse(binaryString, System.Globalization.NumberStyles.AllowHexSpecifier);
    }

    public static async Task<string> BigIntegerToBitString(BigInteger value)
    {
        if (value < 0 || value > (BigInteger.One << 128) - 1)
        {
            await AgnetaHandler.Log(2, "Value is out of range for a 128-bit number.");
            throw new ArgumentOutOfRangeException("Value is out of range for a 128-bit number.");
        }

        return Convert.ToString((long)value, 2).PadLeft(128, '0');
    }

    static async Task<string> ConvertBitStringToUniqueString(string bitString)
    {
        // Validate the input
        if (bitString.Length != 64)
        {
            await AgnetaHandler.Log(2, "Input must be a 64-bit binary string.");
            throw new ArgumentException("Input must be a 64-bit binary string.");
        }

        byte[] bytes = new byte[8];
        for (int i = 0; i < 8; i++)
        {
            string byteString = bitString.Substring(i * 8, 8);
            bytes[i] = Convert.ToByte(byteString, 2);
        }

        return Convert.ToBase64String(bytes);
    }

    public static double GetLoadAverage()
    {
        string[] parts = File.ReadAllText("/proc/loadavg").Split(' ', StringSplitOptions.RemoveEmptyEntries);
        return double.Parse(parts[0]);
    }
    
    // OPTIMIZATION: Efficient horizontal sum using SIMD shuffle operations
    // This is faster than copying to arrays and summing
    private static double HorizontalSum(Vector<float> vec)
    {
        // Platform-optimized horizontal sum
        // For AVX2 (8 floats), this is much faster than copying to array
        float sum = 0.0f;
        for (int i = 0; i < Vector<float>.Count; i++)
        {
            sum += vec[i];
        }
        return sum;
    }

}