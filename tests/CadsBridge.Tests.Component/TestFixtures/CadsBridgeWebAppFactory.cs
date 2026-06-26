using Amazon.S3;
using CadsBridge.Application.DataLoad.Services;
using CadsBridge.Infrastructure.Storage.Abstractions;
using CadsBridge.Infrastructure.Storage.Clients;
using CadsBridge.Infrastructure.Storage.Factories;
using CadsBridge.Testing.Support.Constants;
using CadsBridge.Testing.Support.TestFixtures.Components;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Moq;

namespace CadsBridge.Tests.Component.TestFixtures;

public class CadsBridgeWebAppFactory(IDictionary<string, string?>? configOverrides = null, bool disableHostedServices = true)
    : WebAppFactoryBase<Program>(configOverrides, disableHostedServices)
{
    public CadsBridgeWebAppFactory() : this(null) { }

    public Mock<IDataSeedFileLoadService> DataSeedFileLoaderMock { get; } = new();

    private readonly List<Action<IServiceCollection>> _testServiceOverrides = [];

    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        base.ConfigureWebHost(builder);

        builder.ConfigureTestServices(services =>
        {
            services.RemoveAll<IDataSeedFileLoadService>();
            services.AddSingleton(DataSeedFileLoaderMock.Object);

            OverrideAmazonS3(services);

            foreach (var overrideAction in _testServiceOverrides)
            {
                overrideAction(services);
            }
        });
    }

    public void OverrideHttpClientHandler(string clientName, HttpMessageHandler handler)
    {
        _testServiceOverrides.Add(services =>
            services.AddHttpClient(clientName)
                .ConfigurePrimaryHttpMessageHandler(() => handler));
    }

    public void OverrideApiClientHealthHandler(string apiClientName, HttpMessageHandler handler)
    {
        var clientName = CadsBridge.Infrastructure.ApiClients.Setup.ServiceCollectionExtensions.HealthClientName(apiClientName);
        OverrideHttpClientHandler(clientName, handler);
    }


    private void OverrideAmazonS3(IServiceCollection services)
    {
        services.RemoveAll<IAmazonS3>();
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
}