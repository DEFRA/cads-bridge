using CadsBridge.Application.Messaging.Clients;
using CadsBridge.Application.Messaging.Publishers;
using CadsBridge.Infrastructure.Messaging.Configuration;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

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
        services.AddSingleton<IMessagePublisher<CadsBridgeFifoQueueClient>, CadsBridgeFifoQueuePublisher>();
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