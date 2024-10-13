
using Google.Protobuf;
using System.Text.Json;
using System.Net;
using Agent.Models.Misc;

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

    private static string GetLocalIPAddress()
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
}