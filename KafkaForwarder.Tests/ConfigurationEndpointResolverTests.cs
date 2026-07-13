using System.Text.Json;
using Microsoft.Extensions.Options;
using Xunit;

public class ConfigurationEndpointResolverTests
{
    private static ConfigurationEndpointResolver CreateResolver(ForwardingSettings settings) =>
        new ConfigurationEndpointResolver(Options.Create(settings));

    [Fact]
    public void Resolve_ByType_ReturnsMappedEndpoint()
    {
        var settings = new ForwardingSettings
        {
            DefaultEndpoint = "http://default.local",
            Endpoints = new System.Collections.Generic.Dictionary<string, string>
            {
                ["order_created"] = "http://api.local/actions/order"
            }
        };
        var resolver = CreateResolver(settings);

        using var doc = JsonDocument.Parse("{\"type\":\"order_created\",\"id\":123}");
        var endpoint = resolver.Resolve(doc.RootElement);

        Assert.Equal("http://api.local/actions/order", endpoint);
    }

    [Fact]
    public void Resolve_ByAction_ReturnsMappedEndpoint()
    {
        var settings = new ForwardingSettings
        {
            DefaultEndpoint = "http://default.local",
            Endpoints = new System.Collections.Generic.Dictionary<string, string>
            {
                ["user_registered"] = "http://api.local/actions/user"
            }
        };
        var resolver = CreateResolver(settings);

        using var doc = JsonDocument.Parse("{\"action\":\"user_registered\",\"user\":{\"name\":\"alice\"}}");
        var endpoint = resolver.Resolve(doc.RootElement);

        Assert.Equal("http://api.local/actions/user", endpoint);
    }

    [Fact]
    public void Resolve_UsesDefault_WhenNoMatch()
    {
        var settings = new ForwardingSettings
        {
            DefaultEndpoint = "http://default.local",
            Endpoints = new System.Collections.Generic.Dictionary<string, string>
            {
                ["order_created"] = "http://api.local/actions/order"
            }
        };
        var resolver = CreateResolver(settings);

        using var doc = JsonDocument.Parse("{\"some\":\"value\"}");
        var endpoint = resolver.Resolve(doc.RootElement);

        Assert.Equal("http://default.local", endpoint);
    }

    [Fact]
    public void Resolve_ReturnsEmpty_WhenNoDefaultAndNoEndpoints()
    {
        var settings = new ForwardingSettings
        {
            DefaultEndpoint = null,
            Endpoints = null
        };
        var resolver = CreateResolver(settings);

        using var doc = JsonDocument.Parse("{\"type\":\"unknown\"}");
        var endpoint = resolver.Resolve(doc.RootElement);

        Assert.Equal(string.Empty, endpoint);
    }
}
