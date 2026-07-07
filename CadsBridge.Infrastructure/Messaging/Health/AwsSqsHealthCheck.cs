using Amazon.SQS;
using Amazon.SQS.Model;
using CadsBridge.Infrastructure.Messaging.Configuration;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;

namespace CadsBridge.Infrastructure.Messaging.Health;

public class AwsSqsHealthCheck<T>(IOptionsMonitor<T> options, IAmazonSQS sqs) : IHealthCheck
    where T : QueueConsumerOptions
{
    private readonly IOptionsMonitor<T> _options = options;
    private readonly IAmazonSQS _sqs = sqs;

    public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        var queueOptions = _options.Get(context.Registration.Name);

        var queueUrl = queueOptions.QueueUrl;
        var timeout = TimeSpan.FromSeconds(queueOptions.WaitTimeSeconds);

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(timeout);

        Exception? exception = null;
        GetQueueAttributesResponse? attributes = null;

        try
        {
            attributes = await _sqs.GetQueueAttributesAsync(queueUrl, ["All"], cancellationToken);
        }
        catch (TaskCanceledException)
        {
            exception = new TimeoutException($"The queue check was cancelled, probably because it timed out after {queueOptions.WaitTimeSeconds} seconds");
        }
        catch (Exception ex)
        {
            exception = ex;
        }

        var healthStatus = attributes != null ? HealthStatus.Healthy : HealthStatus.Degraded;

        var data = attributes == null ? [] : new Dictionary<string, object>
        {
            { "queue-url", queueUrl },
            { "approximate-number-of-messages", attributes.ApproximateNumberOfMessages },
            { "approximate-number-of-messages-delayed", attributes.ApproximateNumberOfMessagesDelayed },
            { "approximate-number-of-messages-not-visible", attributes.ApproximateNumberOfMessagesNotVisible },
            { "content-length", attributes.ContentLength }
        };

        if (exception != null)
        {
            healthStatus = HealthStatus.Unhealthy;
            data.Add("error", $"{exception.Message} - {exception.InnerException?.Message}");
        }

        return new HealthCheckResult(
            status: healthStatus,
            description: $"Health check on queue: {queueUrl}",
            exception: exception,
            data: data);
    }
}