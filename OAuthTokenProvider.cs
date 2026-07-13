using System;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

public class OAuthTokenProvider : ITokenProvider
{
    private readonly IHttpClientFactory _httpFactory;
    private readonly DynamicApiSettings _settings;
    private readonly ILogger<OAuthTokenProvider> _logger;

    private readonly SemaphoreSlim _lock = new SemaphoreSlim(1,1);
    private string? _accessToken;
    private DateTimeOffset _expiresAt = DateTimeOffset.MinValue;

    public OAuthTokenProvider(IHttpClientFactory httpFactory, IOptions<DynamicApiSettings> options, ILogger<OAuthTokenProvider> logger)
    {
        _httpFactory = httpFactory;
        _settings = options.Value;
        _logger = logger;
    }

    public async Task<string?> GetTokenAsync(CancellationToken ct)
    {
        if (!_settings.UseOAuth)
            return null;

        if (!string.IsNullOrEmpty(_accessToken) && DateTimeOffset.UtcNow < _expiresAt)
            return _accessToken;

        await _lock.WaitAsync(ct);
        try
        {
            if (!string.IsNullOrEmpty(_accessToken) && DateTimeOffset.UtcNow < _expiresAt)
                return _accessToken;

            if (string.IsNullOrEmpty(_settings.TokenUrl) || string.IsNullOrEmpty(_settings.ClientId) || string.IsNullOrEmpty(_settings.ClientSecret))
            {
                _logger.LogWarning("OAuth settings incomplete; cannot acquire token.");
                return null;
            }

            var client = _httpFactory.CreateClient("dynamic");

            // Use HTTP Basic auth for token endpoint as required: Authorization: Basic base64(clientId:clientSecret)
            var basic = Convert.ToBase64String(Encoding.UTF8.GetBytes($"{_settings.ClientId}:{_settings.ClientSecret}"));
            var req = new HttpRequestMessage(HttpMethod.Post, _settings.TokenUrl);
            req.Headers.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Basic", basic);

            // Body: only grant_type and optional scope (no client_id/client_secret in body)
            var body = new System.Collections.Generic.List<System.Collections.Generic.KeyValuePair<string,string>>
            {
                new System.Collections.Generic.KeyValuePair<string,string>("grant_type","client_credentials")
            };
            if (!string.IsNullOrEmpty(_settings.Scope))
            {
                body.Add(new System.Collections.Generic.KeyValuePair<string,string>("scope", _settings.Scope));
            }

            req.Content = new FormUrlEncodedContent(body);

            var resp = await client.SendAsync(req, ct);
            if (!resp.IsSuccessStatusCode)
            {
                _logger.LogWarning("Token endpoint returned {Status}", resp.StatusCode);
                return null;
            }

            var json = await resp.Content.ReadAsStringAsync(ct);
            using var doc = JsonDocument.Parse(json);
            if (doc.RootElement.TryGetProperty("access_token", out var tokenEl))
            {
                _accessToken = tokenEl.GetString();
                var expiresIn = 3600;
                if (doc.RootElement.TryGetProperty("expires_in", out var expEl) && expEl.TryGetInt32(out var ei))
                    expiresIn = ei;
                // set earlier expiry to provide refresh buffer
                _expiresAt = DateTimeOffset.UtcNow.AddSeconds(expiresIn - 60);
                _logger.LogInformation("Acquired access token, expires in {Seconds}s", expiresIn);
                return _accessToken;
            }

            _logger.LogWarning("Token response did not contain access_token");
            return null;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to acquire token");
            return null;
        }
        finally
        {
            _lock.Release();
        }
    }
}
