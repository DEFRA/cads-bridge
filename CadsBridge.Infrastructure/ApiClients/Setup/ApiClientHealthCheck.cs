using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

namespace CadsBridge.Infrastructure.ApiClients.Setup;

public class ApiClientHealthCheck(
    IHttpClientFactory httpClientFactory,
    string httpClientName,
    string displayName,
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
            var client = httpClientFactory.CreateClient(httpClientName);
            response = await client.GetAsync("/health", cts.Token);
        }
        catch (TaskCanceledException)
        {
            exception = new TimeoutException($"Health check for '{displayName}' timed out after {_timeout.TotalSeconds} seconds.");
            logger?.LogWarning(exception,
                "Health check timed out for '{DisplayName}' using probe client '{ProbeClientName}'",
                displayName, httpClientName);
        }
        catch (Exception ex)
        {
            exception = ex;
            logger?.LogError(ex,
                "Health check failed for '{DisplayName}' using probe client '{ProbeClientName}'",
                displayName, httpClientName);
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
            { "client-name", displayName },
            { "endpoint", "/health" },
            { "status-code", response?.StatusCode ?? System.Net.HttpStatusCode.Unused },
            { "reason", response?.ReasonPhrase ?? string.Empty }
        };
        if (exception != null)
            data["error"] = $"{exception.Message} - {exception.InnerException?.Message}";

        return new HealthCheckResult(status, $"Health check for HTTP client '{displayName}'", exception, data);
    }
}