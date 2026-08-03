using CadsBridge.Application.Messaging.Clients;
using CadsBridge.Application.Messaging.Publishers;
using CadsBridge.Core.Exceptions;
using CadsBridge.Infrastructure.Messaging.Configuration;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Polly;
using Polly.Retry;

namespace CadsBridge.Infrastructure.Messaging.Publishers.Setup;

public static class ServiceCollectionExtensions
{
    public static void AddMessagePublishers(this IServiceCollection services, IConfigurationSection queueSection)
    {
        var queueConfigs = queueSection.Get<Dictionary<string, QueuePublisherOptions>>();
        if (queueConfigs == null) return;

        foreach (var (_, queueOptions) in queueConfigs)
        {
            services.AddQueuePublisherOptions(queueOptions);
        }

        services.AddSingleton<CadsBridgeFifoQueueClient>();
        services.AddSingleton<CadsBridgeFifoQueuePublisher>();

        var retryPipeline = new ResiliencePipelineBuilder()
            .AddRetry(new RetryStrategyOptions
            {
                ShouldHandle = new PredicateBuilder()
                    .Handle<PublishFailedException>(ex => ex.IsTransient),
                MaxRetryAttempts = 3,
                BackoffType = DelayBackoffType.Exponential,
                Delay = TimeSpan.FromSeconds(1) // 1s, 2s, 4s
            })
            .Build();

        services.AddSingleton<IMessagePublisher<CadsBridgeFifoQueueClient>>(sp =>
            new RetryingMessagePublisher<CadsBridgeFifoQueueClient>(
                sp.GetRequiredService<CadsBridgeFifoQueuePublisher>(),
                retryPipeline));
    }

    private static void AddQueuePublisherOptions(this IServiceCollection services, QueuePublisherOptions queueOptions)
    {
        services.Configure(queueOptions.Name, (QueuePublisherOptions opts) =>
        {
            opts.Name = queueOptions.Name;
            opts.QueueUrl = queueOptions.QueueUrl;
        });
    }
}