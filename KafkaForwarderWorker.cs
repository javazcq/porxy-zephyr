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
    private readonly IMessageForwarder _messageForwarder;

    // Track highest processed offset per partition (next offset to commit)
    private readonly ConcurrentDictionary<TopicPartition, Offset> _processedOffsets = new();

    public KafkaForwarderWorker(
        ILogger<KafkaForwarderWorker> logger,
        IHttpClientFactory httpFactory,
        IEndpointResolver resolver,
        IOptions<KafkaSettings> kafkaOptions,
        IOptions<ForwardingSettings> forwardingOptions,
        IMessageForwarder messageForwarder)
    {
        _logger = logger;
        _httpFactory = httpFactory;
        _resolver = resolver;
        _kafkaSettings = kafkaOptions.Value;
        _forwardingSettings = forwardingOptions.Value;
        _semaphore = new SemaphoreSlim(_forwardingSettings.MaxConcurrency);
        _messageForwarder = messageForwarder;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = _kafkaSettings.BootstrapServers,
            GroupId = _kafkaSettings.GroupId,
            EnableAutoCommit = false, // manual commit after processing
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

                // Wait for concurrency slot
                await _semaphore.WaitAsync(stoppingToken);

                // Launch background processing task
                _ = Task.Run(async () =>
                {
                    try
                    {
                        var success = await _messageForwarder.ForwardAsync(consumeResult, stoppingToken);
                        // mark offset as processed (next offset to commit) regardless of success here; configurable
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

                // Commit any processed offsets (do it from the consumer thread)
                await CommitProcessedOffsetsAsync(consumer);
            }
        }
        finally
        {
            // final commit before shutdown
            await CommitProcessedOffsetsAsync(consumer);
            consumer.Close();
            _logger.LogInformation("Consumer closed.");
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
            // remove committed entries
            foreach (var tpo in offsetsToCommit)
            {
                _processedOffsets.TryRemove(tpo.TopicPartition, out _);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to commit offsets");
            // keep entries for next attempt
        }

        return Task.CompletedTask;
    }
}
