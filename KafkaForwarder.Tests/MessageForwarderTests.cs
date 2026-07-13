using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Xunit;

public class MessageForwarderTests
{
    private static HttpClient CreateClientWithHandler(FakeHttpMessageHandler handler)
    {
        return new HttpClient(handler)
        {
            BaseAddress = new Uri("http://localhost") // not used, we send absolute URIs
        };
    }

    private static IHttpClientFactory CreateFactoryReturning(HttpClient client)
    {
        return new SimpleHttpClientFactory(client);
    }

    [Fact]
    public async Task ForwardAsync_SendsPostToResolvedEndpoint()
    {
        // Arrange
        var settings = Options.Create(new ForwardingSettings { RetryCount = 1, RetryBaseSeconds = 0.1 });
        var endpoints = new Dictionary<string, string> { ["order_created"] = "http://api.local/actions/order" };
        var resolver = new ConfigurationEndpointResolver(Options.Create(new ForwardingSettings { Endpoints = endpoints, DefaultEndpoint = null }));

        var handler = new FakeHttpMessageHandler(new[] { new HttpResponseMessage(HttpStatusCode.OK) });
        var client = CreateClientWithHandler(handler);
        var factory = CreateFactoryReturning(client);

        var forwarder = new MessageForwarder(factory, resolver, settings, NullLogger<MessageForwarder>.Instance);

        var msgJson = "{\"type\":\"order_created\",\"id\":1}";
        var cr = new ConsumeResult<string, string>
        {
            Message = new Message<string, string> { Key = "k", Value = msgJson },
            TopicPartitionOffset = new TopicPartitionOffset("topic", 0, 5)
        };

        // Act
        var result = await forwarder.ForwardAsync(cr, CancellationToken.None);

        // Assert
        Assert.True(result);
        Assert.Single(handler.Requests);
        var req = handler.Requests[0];
        Assert.Equal("http://api.local/actions/order", req.RequestUri.ToString());
        var body = await req.Content.ReadAsStringAsync();
        Assert.Equal(msgJson, body);
        Assert.Equal("application/json; charset=utf-8", req.Content.Headers.ContentType!.ToString());
    }

    [Fact]
    public async Task ForwardAsync_RetriesOnFailure_ThenSucceeds()
    {
        // Arrange
        var settings = Options.Create(new ForwardingSettings { RetryCount = 3, RetryBaseSeconds = 0.01 });
        var endpoints = new Dictionary<string, string> { ["order_created"] = "http://api.local/actions/order" };
        var resolver = new ConfigurationEndpointResolver(Options.Create(new ForwardingSettings { Endpoints = endpoints }));

        // simulate two failures then success
        var responses = new[]
        {
            new HttpResponseMessage(HttpStatusCode.InternalServerError),
            new HttpResponseMessage(HttpStatusCode.BadGateway),
            new HttpResponseMessage(HttpStatusCode.OK)
        };
        var handler = new FakeHttpMessageHandler(responses);
        var client = CreateClientWithHandler(handler);
        var factory = CreateFactoryReturning(client);

        var forwarder = new MessageForwarder(factory, resolver, settings, NullLogger<MessageForwarder>.Instance);

        var msgJson = "{\"type\":\"order_created\",\"id\":2}";
        var cr = new ConsumeResult<string, string>
        {
            Message = new Message<string, string> { Key = "k", Value = msgJson },
            TopicPartitionOffset = new TopicPartitionOffset("topic", 0, 8)
        };

        // Act
        var result = await forwarder.ForwardAsync(cr, CancellationToken.None);

        // Assert
        Assert.True(result);
        Assert.Equal(3, handler.Requests.Count); // two retries then success
    }

    [Fact]
    public async Task ForwardAsync_NoEndpointResolved_DoesNotSend()
    {
        // Arrange
        var settings = Options.Create(new ForwardingSettings { RetryCount = 1, RetryBaseSeconds = 0.01 });
        var resolver = new ConfigurationEndpointResolver(Options.Create(new ForwardingSettings { Endpoints = null, DefaultEndpoint = null }));
        var handler = new FakeHttpMessageHandler(new[] { new HttpResponseMessage(HttpStatusCode.OK) });
        var client = CreateClientWithHandler(handler);
        var factory = CreateFactoryReturning(client);
        var forwarder = new MessageForwarder(factory, resolver, settings, NullLogger<MessageForwarder>.Instance);

        var msgJson = "{\"type\":\"unknown_event\"}";
        var cr = new ConsumeResult<string, string>
        {
            Message = new Message<string, string> { Key = "k", Value = msgJson },
            TopicPartitionOffset = new TopicPartitionOffset("topic", 0, 9)
        };

        // Act
        var result = await forwarder.ForwardAsync(cr, CancellationToken.None);

        // Assert
        Assert.False(result);
        Assert.Empty(handler.Requests);
    }
}
