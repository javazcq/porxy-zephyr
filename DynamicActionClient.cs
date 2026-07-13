using System;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

public class DynamicActionClient : IDynamicActionClient
{
    private readonly IHttpClientFactory _httpFactory;
    private readonly DynamicApiSettings _settings;
    private readonly ILogger<DynamicActionClient> _logger;

    public DynamicActionClient(IHttpClientFactory httpFactory, IOptions<DynamicApiSettings> options, ILogger<DynamicActionClient> logger)
    {
        _httpFactory = httpFactory;
        _settings = options.Value;
        _logger = logger;
    }

    public async Task<bool> InvokeActionAsync(string actionName, string jsonPayload, CancellationToken ct)
    {
        if (string.IsNullOrEmpty(_settings.BaseUrl))
        {
            _logger.LogWarning("Dynamic API base URL not configured.");
            return false;
        }

        var endpoint = $"{_settings.BaseUrl.TrimEnd('/')}/actions/{Uri.EscapeDataString(actionName)}";
        var client = _httpFactory.CreateClient("dynamic");

        using var content = new StringContent(jsonPayload ?? string.Empty, Encoding.UTF8, "application/json");

        if (!string.IsNullOrEmpty(_settings.ApiKey))
            client.DefaultRequestHeaders.TryAddWithoutValidation("Authorization", $"Bearer {_settings.ApiKey}");

        try
        {
            var resp = await client.PostAsync(endpoint, content, ct);
            if (resp.IsSuccessStatusCode)
            {
                _logger.LogInformation("Dynamic action {Action} invoked successfully (Status {Status})", actionName, resp.StatusCode);
                return true;
            }
            _logger.LogWarning("Dynamic action {Action} invocation failed (Status {Status})", actionName, resp.StatusCode);
            return false;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Exception invoking dynamic action {Action}", actionName);
            return false;
        }
    }
}
