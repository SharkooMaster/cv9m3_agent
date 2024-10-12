
using Google.Protobuf;
using System.Text.Json;
using System.Net;

namespace Agent.Utils.Misc;

public class Metadata
{
    public string Environment { get; set; }
}

public class ServiceData
{
    public string Id { get; set; }
    public string Name { get; set; }
    public string Host { get; set; }
    public string Port { get; set; }
    public string Url { get; set; }
    public string HealthCheck { get; set; }
    public string Version { get; set; }
    public Metadata Metadata { get; set; }
}

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
}