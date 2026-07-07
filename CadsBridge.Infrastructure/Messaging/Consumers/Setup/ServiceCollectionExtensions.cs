using CadsBridge.Application.Messaging.Messages;
using CadsBridge.Application.Messaging.Observers;
using CadsBridge.Application.Messaging.Serializers;
using CadsBridge.Infrastructure.Messaging.Configuration;
using CadsBridge.Infrastructure.Messaging.Consumers.Observers;
using CadsBridge.Infrastructure.Messaging.Extensions;
using CadsBridge.Infrastructure.Messaging.Factories;
using CadsBridge.Infrastructure.Messaging.Health;
using CadsBridge.Infrastructure.Messaging.Serializers;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace CadsBridge.Infrastructure.Messaging.Consumers.Setup;

public static class ServiceCollectionExtensions
{
    public static void AddMessageConsumers(this IServiceCollection services, IConfigurationSection queueSection)
    {
        var queueConfigs = queueSection.Get<Dictionary<string, QueueConsumerOptions>>();
        if (queueConfigs == null) return;

        foreach (var (_, queueOptions) in queueConfigs)
        {
            services.AddQueueConsumerOptions(queueOptions);
        }

        services.AddMessageHandlers();

        services.AdddMessageSerializers();

        services.AddMessageConsumers();

        // Register DLQ services
    }

    private static void AddQueueConsumerOptions(this IServiceCollection services, QueueConsumerOptions queueOptions)
    {
        services.Configure(queueOptions.Name, (QueueConsumerOptions opts) =>
        {
            opts.Name = queueOptions.Name;
            opts.QueueUrl = queueOptions.QueueUrl;
            opts.DlqQueueUrl = queueOptions.DlqQueueUrl;
            opts.MaxNumberOfMessages = queueOptions.MaxNumberOfMessages;
            opts.WaitTimeSeconds = queueOptions.WaitTimeSeconds;
            opts.HealthcheckEnabled = queueOptions.HealthcheckEnabled;
        });

        if (queueOptions.HealthcheckEnabled)
        {
            services.AddTransient<AwsSqsHealthCheck<QueueConsumerOptions>>();

            services.AddHealthChecks()
                .AddCheck<AwsSqsHealthCheck<QueueConsumerOptions>>(
                    name: queueOptions.Name,
                    tags: ["aws", "sqs"]
                );
        }
    }

    private static IServiceCollection AddMessageHandlers(this IServiceCollection services)
    {
        services.AddSingleton(sp =>
        {
            var registry = new MessageCommandRegistry();

            registry.Register<CsvDataFileImportMessageCommandFactory>(nameof(CsvDataFileImportMessage).ReplaceSuffix());

            return registry;
        });

        return services;
    }

    private static void AdddMessageSerializers(this IServiceCollection services)
    {
        var messageIdentifierTypes = new[]
        {
            typeof(CsvDataFileImportMessage)
        };

        foreach (var messageType in messageIdentifierTypes)
        {
            var typeInfo = MessageIdentifierSerializerContext.Default.GetType().GetProperty(messageType.Name)?.GetValue(MessageIdentifierSerializerContext.Default);

            var serializerType = typeof(MessageIdentifierSerializer<>).MakeGenericType(messageType);
            var interfaceType = typeof(IUnwrappedMessageSerializer<>).MakeGenericType(messageType);

            services.AddSingleton(interfaceType, Activator.CreateInstance(serializerType, typeInfo)!);
        }
    }

    private static void AddMessageConsumers(this IServiceCollection services)
    {
        // Register pollers
        // Register hosted services

        services.AddTransient<IQueuePollerObserver<MessageType>, NullQueuePollerObserver<MessageType>>();
    }
}