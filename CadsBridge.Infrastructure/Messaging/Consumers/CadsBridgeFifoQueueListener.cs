using CadsBridge.Application.Messaging.Clients;
using CadsBridge.Application.Messaging.Consumers;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace CadsBridge.Infrastructure.Messaging.Consumers;

public class CadsBridgeFifoQueueListener(
    IQueuePoller<CadsBridgeFifoQueueClient> queuePoller,
    IConfiguration configuration,
    ILogger<CadsBridgeFifoQueueListener> logger)
    : IHostedService
{
    /// <summary>
    /// When <c>Messaging:DisableQueueConsumer</c> is <c>true</c> the listener skips
    /// starting the underlying poller. Intended for integration-test scenarios where the
    /// BulkScanJob publisher must be observable without the consumer racing to drain the
    /// queue.
    /// </summary>
    private bool IsDisabled => configuration.GetValue<bool>("Messaging:DisableQueueConsumer");

    public Task StartAsync(CancellationToken cancellationToken)
    {
        if (IsDisabled)
        {
            logger.LogInformation("CadsBridgeFifoQueueListener is disabled via configuration.");
            return Task.CompletedTask;
        }

        logger.LogInformation("CadsBridgeFifoQueueListener start requested.");
        return queuePoller.StartAsync(cancellationToken);
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (IsDisabled) return;

        logger.LogInformation("CadsBridgeFifoQueueListener stop requested.");

        try
        {
            await queuePoller.StopAsync(cancellationToken);
        }
        catch (TaskCanceledException)
        {
            // Swallow expected cancellation
        }
        catch (ObjectDisposedException)
        {
            // Swallow: poller was already disposed (e.g. by the DI container) before
            // the hosted-service stop sequence completed.
        }
    }
}