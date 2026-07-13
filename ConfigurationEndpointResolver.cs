using System;
using System.Text.Json;
using Microsoft.Extensions.Options;
using System.Collections.Generic;

public class ConfigurationEndpointResolver : IEndpointResolver
{
    private readonly ForwardingSettings _settings;
    public ConfigurationEndpointResolver(IOptions<ForwardingSettings> options)
    {
        _settings = options.Value;
    }

    public string Resolve(JsonElement messageRoot)
    {
        if (messageRoot.TryGetProperty("type", out var typeProp) && typeProp.ValueKind == JsonValueKind.String)
        {
            var key = typeProp.GetString() ?? "";
            if (_settings.Endpoints != null && _settings.Endpoints.TryGetValue(key, out var endpoint))
                return endpoint;
        }

        if (messageRoot.TryGetProperty("action", out var actProp) && actProp.ValueKind == JsonValueKind.String)
        {
            var key = actProp.GetString() ?? "";
            if (_settings.Endpoints != null && _settings.Endpoints.TryGetValue(key, out var endpoint))
                return endpoint;
        }

        return _settings.DefaultEndpoint ?? string.Empty;
    }
}
