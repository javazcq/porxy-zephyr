using System.Collections.Generic;

public class KafkaSettings
{
    public string BootstrapServers { get; set; } = "localhost:9092";
    public string Topic { get; set; } = "my-topic";
    public string GroupId { get; set; } = "my-group";
}

public class ForwardingSettings
{
    public int MaxConcurrency { get; set; } = 4;
    public int RetryCount { get; set; } = 3;
    public double RetryBaseSeconds { get; set; } = 1.0;
    public string? DefaultEndpoint { get; set; }
    public Dictionary<string, string>? Endpoints { get; set; }
}

public class DynamicApiSettings
{
    public string? BaseUrl { get; set; }
    public string? ApiKey { get; set; }
}
