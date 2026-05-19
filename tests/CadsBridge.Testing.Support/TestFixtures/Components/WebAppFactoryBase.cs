using System.Net;
using Amazon.S3;
using Amazon.S3.Model;
using CadsBridge.Core.Storage.Abstractions;
using CadsBridge.Core.Storage.Clients;
using CadsBridge.Core.Storage.Factories;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Moq;

namespace CadsBridge.Testing.Support.TestFixtures.Components;

public abstract class WebAppFactoryBase<TStart>(
    IDictionary<string, string?>? configOverrides = null) : WebApplicationFactory<TStart>
    where TStart : class
{
    public Mock<IAmazonS3> AmazonS3Mock { get; private set; } = new();
    private readonly IDictionary<string, string?> _configOverrides = configOverrides ?? new Dictionary<string, string?>();

    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        builder.UseSetting(WebHostDefaults.ApplicationKey, typeof(TStart).Assembly.FullName);
        builder.UseContentRoot(AppContext.BaseDirectory);
        builder.UseEnvironment("Test");

        SetTestEnvironmentVariables();

        builder.ConfigureAppConfiguration((_, configBuilder) =>
        {
            if (_configOverrides.Count > 0)
                configBuilder.AddInMemoryCollection(_configOverrides);
        });

        builder.ConfigureTestServices(services =>
        {
            OverrideAmazonS3(services);
        });

        builder.ConfigureServices(services =>
        {
            services.AddSingleton(AmazonS3Mock.Object);
            services.RemoveAll<IHostedService>();
        });
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
        Environment.SetEnvironmentVariable("Storage__Internal__BucketName", "cads-bridge-internal-bucket");
        Environment.SetEnvironmentVariable("Storage__External__BucketName", "cads-bridge-external-bucket");
        Environment.SetEnvironmentVariable("IMB_S3_ACCESS_KEY", "test");
        Environment.SetEnvironmentVariable("IMB_S3_ACCESS_SECRET", "test");
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

            factory.RegisterMockClient<InternalStorageClient>(
                "cads-internal-bucket",
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
}