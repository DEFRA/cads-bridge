using CadsBridge.Core.Correlation;

namespace CadsBridge.Infrastructure.ApiClients.Handlers;

/// <summary>
/// A <see cref="DelegatingHandler"/> that stamps every outgoing request with the current
/// correlation id taken from <see cref="CorrelationIdContext"/>.
/// <para>
/// This runs at send-time (not when the <see cref="HttpClient"/> is built), so it always
/// reflects the correlation id flowing through the current async context - even though the
/// same <see cref="HttpClient"/> instance may be reused across many units of work.
/// </para>
/// </summary>
public class CorrelationIdHandler : DelegatingHandler
{
    public const string HeaderName = "x-cdp-request-id";

    protected override Task<HttpResponseMessage> SendAsync(
        HttpRequestMessage request,
        CancellationToken cancellationToken)
    {
        var correlationId = CorrelationIdContext.Value;

        if (!string.IsNullOrWhiteSpace(correlationId))
        {
            // Remove any pre-existing value to avoid duplicates on retries.
            request.Headers.Remove(HeaderName);
            request.Headers.Add(HeaderName, correlationId);
        }

        return base.SendAsync(request, cancellationToken);
    }
}

