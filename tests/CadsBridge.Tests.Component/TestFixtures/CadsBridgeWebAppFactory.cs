using CadsBridge.Core.DataSeed.Abstractions;
using CadsBridge.Core.Storage.Abstractions;
using CadsBridge.Core.Storage.Clients;
using CadsBridge.Core.Storage.Factories;
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

    public Mock<IDataSeedFileLoader> DataSeedFileLoaderMock { get; } = new();

    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        base.ConfigureWebHost(builder);

        builder.ConfigureTestServices(services =>
        {
            services.RemoveAll<IDataSeedFileLoader>();
            services.AddSingleton(DataSeedFileLoaderMock.Object);
        });
    }
}