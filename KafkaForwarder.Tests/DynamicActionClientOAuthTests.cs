using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;
using Confluent.Kafka;

public class DynamicActionClientOAuthTests
{
    [Fact]
    public async Task InvokeActionAsync_UsesOAuth2_ClientCredentials()
    {
        // Arrange
        var oauthJson = "{\"access_token\":\"tkn123\",\"expires_in\":3600}";
        var tokenResponse = new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = new StringContent(oauthJson, System.Text.Encoding.UTF8, "application/json")
        };
        var actionResponse = new HttpResponseMessage(HttpStatusCode.OK);

        // Fake handler returns token response first, then action response
        var handler = new FakeHttpMessageHandler(new[] { tokenResponse, actionResponse });
        var client = new HttpClient(handler);
        var factory = new SimpleHttpClientFactory(client);

        var settings = Options.Create(new DynamicApiSettings
        {
            BaseUrl = "http://api.dynamic.local",
            UseOAuth = true,
            TokenUrl = "http://api.dynamic.local/oauth2/token",
            ClientId = "cid",
            ClientSecret = "secret",
            Scope = "api"
        });

        var tokenProvider = new OAuthTokenProvider(factory, settings, NullLogger<OAuthTokenProvider>.Instance);
        var dynamicClient = new DynamicActionClient(factory, settings, NullLogger<DynamicActionClient>.Instance, tokenProvider);

        // Act
        var ok = await dynamicClient.InvokeActionAsync("order", "{\"id\":1}", CancellationToken.None);

        // Assert
        Assert.True(ok);
        Assert.Equal(2, handler.Requests.Count);

        var tokenReq = handler.Requests[0];
        Assert.Equal("http://api.dynamic.local/oauth2/token", tokenReq.RequestUri.ToString());
        // token request should be form-encoded
        Assert.Equal("application/x-www-form-urlencoded", tokenReq.Content.Headers.ContentType!.MediaType);
        var tokenBody = await tokenReq.Content.ReadAsStringAsync();
        Assert.Contains("grant_type=client_credentials", tokenBody);
        Assert.Contains("client_id=cid", tokenBody);
        Assert.Contains("client_secret=secret", tokenBody);

        var actionReq = handler.Requests[1];
        Assert.Equal("http://api.dynamic.local/actions/order", actionReq.RequestUri.ToString());
        // Authorization header should be Bearer tkn123
        Assert.Equal("Bearer tkn123", actionReq.Headers.Authorization?.ToString());
    }
}
