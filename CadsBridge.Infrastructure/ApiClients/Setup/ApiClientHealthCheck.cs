using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

namespace CadsBridge.Infrastructure.ApiClients.Setup;

public class ApiClientHealthCheck(
    IHttpClientFactory httpClientFactory,
    string clientName,
    ILogger<ApiClientHealthCheck>? logger = null) : IHealthCheck
{
    private readonly TimeSpan _timeout = TimeSpan.FromSeconds(10);

    public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = new())
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(_timeout);

        Exception? exception = null;
        HttpResponseMessage? response = null;

        try
        {
            var client = httpClientFactory.CreateClient(clientName);
            response = await client.GetAsync("/health", cts.Token);
        }
        catch (TaskCanceledException)
        {
            exception = new TimeoutException($"Health check for '{clientName}' timed out after {_timeout.TotalSeconds} seconds.");
            logger?.LogWarning(exception,
                "Health check timed out for client '{ProbeClientName}'",
                clientName);
        }
        catch (Exception ex)
        {
            exception = ex;
            logger?.LogError(ex,
                "Health check failed for client '{ProbeClientName}'",
                clientName);
        }

        HealthStatus status;
        if (response?.IsSuccessStatusCode == true)
            status = HealthStatus.Healthy;
        else if (response != null)
            status = HealthStatus.Degraded;
        else
            status = HealthStatus.Unhealthy;

        var data = new Dictionary<string, object>
        {
            { "client-name", clientName },
            { "endpoint", "/health" },
            { "status-code", response?.StatusCode ?? System.Net.HttpStatusCode.Unused },
            { "reason", response?.ReasonPhrase ?? string.Empty }
        };
        if (exception != null)
            data["error"] = $"{exception.Message} - {exception.InnerException?.Message}";

        return new HealthCheckResult(status, $"Health check for HTTP client '{clientName}'", exception, data);
    }
}