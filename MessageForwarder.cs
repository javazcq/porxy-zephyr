using System;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Polly;
using Polly.Retry;
using Confluent.Kafka;

public class MessageForwarder : IMessageForwarder
{
    private readonly IHttpClientFactory _httpFactory;
    private readonly IEndpointResolver _resolver;
    private readonly ForwardingSettings _settings;
    private readonly ILogger<MessageForwarder> _logger;
    private readonly AsyncRetryPolicy<HttpResponseMessage> _retryPolicy;

    public MessageForwarder(
        IHttpClientFactory httpFactory,
        IEndpointResolver resolver,
        IOptions<ForwardingSettings> settings,
        ILogger<MessageForwarder> logger)
    {
        _httpFactory = httpFactory;
        _resolver = resolver;
        _settings = settings.Value;
        _logger = logger;

        _retryPolicy = Policy<HttpResponseMessage>
            .Handle<HttpRequestException>()
            .OrResult(r => !r.IsSuccessStatusCode)
            .WaitAndRetryAsync(
                _settings.RetryCount,
                attempt => TimeSpan.FromSeconds(Math.Pow(2, attempt)) * _settings.RetryBaseSeconds,
                onRetry: (outcome, timespan, retryAttempt, ctx) =>
                {
                    _logger.LogWarning("HTTP forward retry {Attempt} after {Delay}. Reason: {Reason}",
                        retryAttempt, timespan, outcome.Exception?.Message ?? outcome.Result.StatusCode.ToString());
                });
    }

    public async Task<bool> ForwardAsync(ConsumeResult<string, string> consumeResult, CancellationToken ct)
    {
        if (consumeResult?.Message == null)
            return false;

        JsonDocument? doc = null;
        try
        {
            doc = JsonDocument.Parse(consumeResult.Message.Value ?? "{}");
        }
        catch (JsonException ex)
        {
            _logger.LogWarning(ex, "Invalid JSON in message at {TPO}; skipping forward", consumeResult.TopicPartitionOffset);
            return false;
        }

        var endpoint = _resolver.Resolve(doc.RootElement);
        if (string.IsNullOrEmpty(endpoint))
        {
            _logger.LogInformation("No endpoint resolved for message at {TPO}; skipping", consumeResult.TopicPartitionOffset);
            return false;
        }

        var client = _httpFactory.CreateClient("forwarder");
        using var content = new StringContent(consumeResult.Message.Value ?? string.Empty, Encoding.UTF8, "application/json");

        try
        {
            var response = await _retryPolicy.ExecuteAsync(async ctInner =>
                await client.PostAsync(endpoint, content, ctInner), ct);

            if (response.IsSuccessStatusCode)
            {
                _logger.LogInformation("Forwarded message {TPO} -> {Endpoint} (Status {Status})", consumeResult.TopicPartitionOffset, endpoint, response.StatusCode);
                return true;
            }
            else
            {
                _logger.LogWarning("Failed to forward message {TPO} -> {Endpoint}. Final status: {Status}", consumeResult.TopicPartitionOffset, endpoint, response.StatusCode);
                return false;
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Exception while forwarding message {TPO} to {Endpoint}", consumeResult.TopicPartitionOffset, endpoint);
            return false;
        }
        finally
        {
            doc?.Dispose();
        }
    }
}
