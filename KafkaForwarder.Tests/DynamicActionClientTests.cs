using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;
using Confluent.Kafka;
using System.Collections.Generic;

public class DynamicActionClientTests
{
    [Fact]
    public async Task InvokeActionAsync_SendsToDynamicApi_WithApiKeyHeader()
    {
        var settings = Options.Create(new DynamicApiSettings { BaseUrl = "http://api.dynamic.local", ApiKey = "tk" });
        var handler = new FakeHttpMessageHandler(new[] { new HttpResponseMessage(HttpStatusCode.OK) });
        var client = new HttpClient(handler);
        var factory = new SimpleHttpClientFactory(client);
        var logger = NullLogger<DynamicActionClient>.Instance;

        var clientImpl = new DynamicActionClient(factory, settings, logger);
        var ok = await clientImpl.InvokeActionAsync("order", "{\"id\":1}", CancellationToken.None);

        Assert.True(ok);
        Assert.Single(handler.Requests);
        var req = handler.Requests[0];
        Assert.Equal("http://api.dynamic.local/actions/order", req.RequestUri.ToString());
        Assert.Equal("Bearer tk", req.Headers.Authorization?.ToString());
    }
}
