using System.Net;

namespace CadsBridge.Testing.Support.Utilities.Http;

public sealed class StubHttpMessageHandler : HttpMessageHandler
{
    private readonly Func<HttpRequestMessage, CancellationToken, HttpResponseMessage> _responder;

    public StubHttpMessageHandler(HttpStatusCode statusCode)
        => _responder = (_, _) => new HttpResponseMessage(statusCode);

    public StubHttpMessageHandler(Exception toThrow)
        => _responder = (_, _) => throw toThrow;

    public StubHttpMessageHandler(Func<HttpRequestMessage, CancellationToken, HttpResponseMessage> responder)
        => _responder = responder;

    public List<HttpRequestMessage> Requests { get; } = new();

    protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        Requests.Add(request);
        try
        {
            return Task.FromResult(_responder(request, cancellationToken));
        }
        catch (Exception ex)
        {
            return Task.FromException<HttpResponseMessage>(ex);  // surface as faulted task
        }
    }
}