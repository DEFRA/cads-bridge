using CadsBridge.Application.Messaging.Clients;
using CadsBridge.Application.Messaging.Consumers;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace CadsBridge.Infrastructure.Messaging.Consumers;

public class CadsBridgeFifoQueueListener(
    IQueuePoller<CadsBridgeFifoQueueClient> queuePoller,
    ILogger<CadsBridgeFifoQueueListener> logger)
    : IHostedService
{
    public Task StartAsync(CancellationToken cancellationToken)
    {
        logger.LogInformation("CadsBridgeFifoQueueListener start requested.");

        return queuePoller.StartAsync(cancellationToken);
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        logger.LogInformation("CadsBridgeFifoQueueListener stop requested.");

        try
        {
            await queuePoller.StopAsync(cancellationToken);
        }
        catch (TaskCanceledException)
        {
            // Swallow expected cancellation
        }
    }
}
