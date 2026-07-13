using System;
using System.Threading;
using System.Threading.Tasks;
using System.Text.Json;
using System.Collections.Concurrent;
using System.Collections.Generic;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Net.Http;
using Confluent.Kafka;
using Polly;
using Polly.Retry;

public class KafkaForwarderWorker : BackgroundService
{
    private readonly ILogger<KafkaForwarderWorker> _logger;
    private readonly IHttpClientFactory _httpFactory;
    private readonly IEndpointResolver _resolver;
    private readonly KafkaSettings _kafkaSettings;
    private readonly ForwardingSettings _forwardingSettings;
    private readonly SemaphoreSlim _semaphore;
    private readonly AsyncRetryPolicy<HttpResponseMessage> _httpRetryPolicy;

    private readonly ConcurrentDictionary<TopicPartition, Offset> _processedOffsets = new();

    public KafkaForwarderWorker(
        ILogger<KafkaForwarderWorker> logger,
        IHttpClientFactory httpFactory,
        IEndpointResolver resolver,
        IOptions<KafkaSettings> kafkaOptions,
        IOptions<ForwardingSettings> forwardingOptions)
    {
        _logger = logger;
        _httpFactory = httpFactory;
        _resolver = resolver;
        _kafkaSettings = kafkaOptions.Value;
        _forwardingSettings = forwardingOptions.Value;
        _semaphore = new SemaphoreSlim(_forwardingSettings.MaxConcurrency);

        _httpRetryPolicy = Policy<HttpResponseMessage>
            .Handle<HttpRequestException>()
            .OrResult(r => !r.IsSuccessStatusCode)
            .WaitAndRetryAsync(
                _forwardingSettings.RetryCount,
                attempt => TimeSpan.FromSeconds(Math.Pow(2, attempt)) * _forwardingSettings.RetryBaseSeconds,
                onRetry: (outcome, timespan, retryAttempt, context) =>
                {
                    _logger.LogWarning("HTTP forward retry {Attempt} after {Delay}. Reason: {Reason}",
                        retryAttempt, timespan, outcome.Exception?.Message ?? outcome.Result.StatusCode.ToString());
                });
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = _kafkaSettings.BootstrapServers,
            GroupId = _kafkaSettings.GroupId,
            EnableAutoCommit = false,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            AllowAutoCreateTopics = false
        };

        using var consumer = new ConsumerBuilder<string, string>(config)
            .SetErrorHandler((_, e) => _logger.LogError("Kafka error: {Reason}", e.Reason))
            .Build();

        consumer.Subscribe(_kafkaSettings.Topic);
        _logger.LogInformation("Subscribed to topic {Topic}", _kafkaSettings.Topic);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                ConsumeResult<string, string>? consumeResult = null;
                try
                {
                    consumeResult = consumer.Consume(stoppingToken);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error while consuming");
                    await Task.Delay(TimeSpan.FromSeconds(1), stoppingToken);
                    continue;
                }

                if (consumeResult == null) continue;

                await _semaphore.WaitAsync(stoppingToken);

                _ = Task.Run(async () =>
                {
                    try
                    {
                        await ProcessMessageAsync(consumeResult, stoppingToken);
                        var tpo = consumeResult.TopicPartition;
                        var nextOffset = consumeResult.Offset + 1;
                        _processedOffsets.AddOrUpdate(tpo, nextOffset, (_, existing) => existing <= nextOffset ? nextOffset : existing);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Unhandled error processing message at {TopicPartitionOffset}", consumeResult.TopicPartitionOffset);
                        var tpo = consumeResult.TopicPartition;
                        var nextOffset = consumeResult.Offset + 1;
                        _processedOffsets.AddOrUpdate(tpo, nextOffset, (_, existing) => existing <= nextOffset ? nextOffset : existing);
                    }
                    finally
                    {
                        _semaphore.Release();
                    }
                }, stoppingToken);

                await CommitProcessedOffsetsAsync(consumer);
            }
        }
        finally
        {
            await CommitProcessedOffsetsAsync(consumer);
            consumer.Close();
            _logger.LogInformation("Consumer closed.");
        }
    }

    private async Task ProcessMessageAsync(ConsumeResult<string, string> cr, CancellationToken ct)
    {
        _logger.LogInformation("Processing message at {TPO}", cr.TopicPartitionOffset);

        using var doc = JsonDocument.Parse(cr.Message.Value ?? "{}");
        var endpoint = _resolver.Resolve(doc.RootElement);

        if (string.IsNullOrEmpty(endpoint))
        {
            _logger.LogWarning("No endpoint resolved for message at {TPO}; skipping", cr.TopicPartitionOffset);
            return;
        }

        var client = _httpFactory.CreateClient("forwarder");
        var content = new StringContent(cr.Message.Value ?? "", System.Text.Encoding.UTF8, "application/json");
        HttpResponseMessage response = await _httpRetryPolicy.ExecuteAsync(async ctInner =>
            await client.PostAsync(endpoint, content, ctInner), ct);

        if (response.IsSuccessStatusCode)
        {
            _logger.LogInformation("Forwarded message {TPO} -> {Endpoint} (Status {Status})", cr.TopicPartitionOffset, endpoint, response.StatusCode);
        }
        else
        {
            _logger.LogWarning("Failed to forward message {TPO} -> {Endpoint} after retries. Status: {Status}", cr.TopicPartitionOffset, endpoint, response.StatusCode);
        }
    }

    private Task CommitProcessedOffsetsAsync(IConsumer<string, string> consumer)
    {
        if (_processedOffsets.IsEmpty) return Task.CompletedTask;

        var offsetsToCommit = new List<TopicPartitionOffset>();
        foreach (var kv in _processedOffsets)
        {
            offsetsToCommit.Add(new TopicPartitionOffset(kv.Key, kv.Value));
        }

        try
        {
            consumer.Commit(offsetsToCommit);
            _logger.LogInformation("Committed {Count} partition offsets", offsetsToCommit.Count);
            foreach (var tpo in offsetsToCommit)
            {
                _processedOffsets.TryRemove(tpo.TopicPartition, out _);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to commit offsets");
        }

        return Task.CompletedTask;
    }
}
