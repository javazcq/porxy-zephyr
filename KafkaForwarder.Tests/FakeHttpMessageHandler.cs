using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

public class FakeHttpMessageHandler : HttpMessageHandler
{
    private readonly System.Collections.Generic.Queue<HttpResponseMessage> _responses;
    public System.Collections.Generic.List<HttpRequestMessage> Requests { get; } = new System.Collections.Generic.List<HttpRequestMessage>();

    public FakeHttpMessageHandler(System.Collections.Generic.IEnumerable<HttpResponseMessage> responses)
    {
        _responses = new System.Collections.Generic.Queue<HttpResponseMessage>(responses);
    }

    protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        Requests.Add(request);

        HttpResponseMessage resp;
        if (_responses.Count > 0)
            resp = _responses.Dequeue();
        else
            resp = new HttpResponseMessage(System.Net.HttpStatusCode.OK);

        return Task.FromResult(resp);
    }
}
