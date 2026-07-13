using System.Net.Http;

public class SimpleHttpClientFactory : IHttpClientFactory
{
    private readonly HttpClient _client;

    public SimpleHttpClientFactory(HttpClient client) => _client = client;

    public HttpClient CreateClient(string name) => _client;
}
