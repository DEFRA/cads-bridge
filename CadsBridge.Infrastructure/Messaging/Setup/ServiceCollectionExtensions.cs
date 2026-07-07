using Amazon.SQS;
using CadsBridge.Infrastructure.Messaging.Consumers.Setup;
using CadsBridge.Infrastructure.Messaging.Factories;
using CadsBridge.Infrastructure.Messaging.Publishers.Setup;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using System.Diagnostics.CodeAnalysis;

namespace CadsBridge.Infrastructure.Messaging.Setup;

public static class ServiceCollectionExtensions
{
    public const string MessagingQueuesConfigSection = "Messaging:Queues";

    public static void AddMessagingDependencies(this IServiceCollection services, IConfiguration configuration)
    {
        services.AddAmazonSQSCore(configuration);

        services.AddTransient<IMessageFactory, MessageFactory>();

        var queueSection = configuration.GetRequiredSection(MessagingQueuesConfigSection);

        services.AddMessageConsumers(queueSection);

        services.AddMessagePublishers(queueSection);
    }

    [ExcludeFromCodeCoverage]
    private static IServiceCollection AddAmazonSQSCore(this IServiceCollection services, IConfiguration configuration)
    {
        if (configuration["LOCALSTACK_ENDPOINT"] != null)
        {
            services.AddSingleton<IAmazonSQS>(sp =>
            {
                var config = new AmazonSQSConfig
                {
                    ServiceURL = configuration["AWS:ServiceURL"],
                    AuthenticationRegion = configuration["AWS:Region"],
                    UseHttp = true
                };
                var credentials = new Amazon.Runtime.BasicAWSCredentials("test", "test");
                return new AmazonSQSClient(credentials, config);
            });
        }
        else
        {
            services.AddAWSService<IAmazonSQS>();
        }

        return services;
    }
}