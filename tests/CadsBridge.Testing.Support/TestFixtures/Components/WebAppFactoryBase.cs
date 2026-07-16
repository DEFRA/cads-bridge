using Amazon.S3;
using Amazon.S3.Model;
using Amazon.SQS;
using Amazon.SQS.Model;
using CadsBridge.Application.Messaging.Clients;
using CadsBridge.Application.Messaging.Consumers;
using CadsBridge.Application.Messaging.Messages;
using CadsBridge.Application.Messaging.Observers;
using CadsBridge.Infrastructure.Messaging.Consumers;
using CadsBridge.Infrastructure.Storage.Abstractions;
using CadsBridge.Infrastructure.Storage.Clients;
using CadsBridge.Infrastructure.Storage.Factories;
using CadsBridge.Testing.Support.Constants;
using CadsBridge.Testing.Support.TestDoubles.Observers;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Moq;
using System.Globalization;
using System.Net;

namespace CadsBridge.Testing.Support.TestFixtures.Components;

public abstract class WebAppFactoryBase<TStart>(
    IDictionary<string, string?>? configOverrides = null,
    bool disableHostedServices = true) : WebApplicationFactory<TStart>
    where TStart : class
{
    public Mock<IAmazonS3> AmazonS3Mock { get; private set; } = new();
    public Mock<IAmazonSQS> AmazonSQSMock { get; private set; } = new();

    public readonly List<Action<IServiceCollection>> _serviceOverrides = [];

    private readonly IDictionary<string, string?> _configOverrides = configOverrides ?? new Dictionary<string, string?>();

    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        var culture = new CultureInfo("en-GB");
        CultureInfo.DefaultThreadCurrentCulture = culture;
        CultureInfo.DefaultThreadCurrentUICulture = culture;

        builder.UseSetting(WebHostDefaults.ApplicationKey, typeof(TStart).Assembly.FullName);
        builder.UseContentRoot(AppContext.BaseDirectory);
        builder.UseEnvironment("Test");

        SetTestEnvironmentVariables();

        // Apply config overrides via UseSetting so they are visible to the application's
        // service-registration code (e.g. AddApiClients reads IConfiguration during ConfigureServices).
        // ConfigureAppConfiguration alone is layered too late for minimal-hosting apps that read
        // configuration while registering services.
        foreach (var (key, value) in _configOverrides)
        {
            builder.UseSetting(key, value);
        }

        builder.ConfigureAppConfiguration((_, configBuilder) =>
        {
            if (_configOverrides.Count > 0)
                configBuilder.AddInMemoryCollection(_configOverrides);
        });

        builder.ConfigureTestServices(services =>
        {
            OverrideAmazonS3(services);
            OverrideAmazonSqs(services);
            ConfigureMessageConsumers(services);
        });

        builder.ConfigureServices(services =>
        {
            if (disableHostedServices)
                services.RemoveAll<IHostedService>();

            foreach (var serviceOverride in _serviceOverrides)
            {
                serviceOverride(services);
            }
        });
    }

    public void OverrideSingleton<T>(T service) where T : class
    {
        _serviceOverrides.Add(x =>
        {
            x.RemoveAll<T>();
            x.AddSingleton<T>(service);
        });
    }

    public void ResetMocks()
    {
        ResetInfrastructureMocks();
    }

    protected override IHost CreateHost(IHostBuilder builder)
    {
        if (_configOverrides.Count > 0)
        {
            builder.ConfigureAppConfiguration(config =>
            {
                config.AddInMemoryCollection(_configOverrides);
            });
        }

        return base.CreateHost(builder);
    }

    private static void SetTestEnvironmentVariables()
    {
        Environment.SetEnvironmentVariable("ASPNETCORE_ENVIRONMENT", "Test");
        Environment.SetEnvironmentVariable("AWS__ServiceURL", "http://cads-bridge-localstack-emulator:4566");

        Environment.SetEnvironmentVariable("Storage__Internal__BucketName", TestS3Constants.TestCadsBridgeInternalBucketName);
        Environment.SetEnvironmentVariable("Storage__External__BucketName", TestS3Constants.TestCadsBridgeExternalBucketName);

        Environment.SetEnvironmentVariable("ApiClients__CdsApi__HealthcheckEnabled", "true");
        Environment.SetEnvironmentVariable("ApiClients__CdsApi__BaseUrl", "http://localhost:5555");
        Environment.SetEnvironmentVariable("ApiClients__CdsApi__BasicApiKey", "XYZ");
        Environment.SetEnvironmentVariable("ApiClients__CdsApi__UseFakeClient", "true");

        Environment.SetEnvironmentVariable("Messaging__Queues__CadsBridgeFifo__QueueUrl", TestSqsConstants.TestQueueUrl);
        Environment.SetEnvironmentVariable("Messaging__Queues__CadsBridgeFifo__DlqQueueUrl", TestSqsConstants.TestQueueDlqUrl);
        Environment.SetEnvironmentVariable("Messaging__Queues__CadsBridgeFifo__HealthcheckEnabled", "true");

        Environment.SetEnvironmentVariable("IMB_S3_ACCESS_KEY", "test");
        Environment.SetEnvironmentVariable("IMB_S3_ACCESS_SECRET", "test");
    }

    private void ResetInfrastructureMocks()
    {
        AmazonS3Mock!.Reset();
        ApplyDefaultS3MockSetup();

        AmazonSQSMock!.Reset();
        ApplyDefaultSqsMockSetup();
    }

    private void OverrideAmazonS3(IServiceCollection services)
    {
        services.RemoveAll<IAmazonS3>();

        ApplyDefaultS3MockSetup();

        services.AddSingleton(AmazonS3Mock.Object);

        services.RemoveAll<IS3ClientFactory>();

        services.AddSingleton<IS3ClientFactory>(sp =>
        {
            var factory = new S3ClientFactory();

            factory.RegisterMockClient<ExternalStorageClient>(
                TestS3Constants.TestCadsBridgeExternalBucketName,
                AmazonS3Mock.Object);
            factory.RegisterMockClient<InternalStorageClient>(
                TestS3Constants.TestCadsBridgeInternalBucketName,
                AmazonS3Mock.Object);

            return factory;
        });
    }

    private void ApplyDefaultS3MockSetup()
    {
        AmazonS3Mock
            .Setup(x => x.GetBucketAclAsync(It.IsAny<GetBucketAclRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new GetBucketAclResponse { HttpStatusCode = HttpStatusCode.OK });

        AmazonS3Mock
            .Setup(x => x.ListObjectsV2Async(It.IsAny<ListObjectsV2Request>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new ListObjectsV2Response { HttpStatusCode = HttpStatusCode.OK });
    }

    private void OverrideAmazonSqs(IServiceCollection services)
    {
        services.RemoveAll<IAmazonSQS>();

        ApplyDefaultSqsMockSetup();

        services.AddSingleton(AmazonSQSMock.Object);
    }

    private void ApplyDefaultSqsMockSetup()
    {
        AmazonSQSMock
            .Setup(x => x.GetQueueAttributesAsync(
                It.IsAny<string>(),
                It.IsAny<List<string>>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(new GetQueueAttributesResponse
            {
                HttpStatusCode = HttpStatusCode.OK
            });

        AmazonSQSMock
            .Setup(x => x.GetQueueAttributesAsync(
                It.IsAny<GetQueueAttributesRequest>(),
                It.IsAny<CancellationToken>()))
            .Throws(new NotImplementedException("Use the (string, List<string>) overload"));
    }

    private static void ConfigureMessageConsumers(IServiceCollection services)
    {
        services.RemoveAll<CadsBridgeFifoQueueListener>();
        services.RemoveAll<TestQueuePollerObserver<MessageType>>();
        services.RemoveAll<IQueuePoller<CadsBridgeFifoQueueClient>>();

        services.AddScoped<IQueuePoller<CadsBridgeFifoQueueClient>, CadsBridgeFifoQueuePoller>();
        services.AddScoped<TestQueuePollerObserver<MessageType>>();
        services.AddScoped<IQueuePollerObserver<MessageType>>(sp => sp.GetRequiredService<TestQueuePollerObserver<MessageType>>());
    }
}