namespace Agent.Models.Misc;

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