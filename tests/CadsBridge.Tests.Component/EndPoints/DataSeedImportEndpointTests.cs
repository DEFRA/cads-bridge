using CadsBridge.Application.DataLoad.Jobs;
using CadsBridge.Core.DataLoad.Seeds;
using CadsBridge.Tests.Component.TestFixtures;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using System.Net;
using System.Net.Http.Headers;
using System.Text.Json;
using System.Threading.Channels;
using CadsBridge.Testing.Support.Utilities.Http;

namespace CadsBridge.Tests.Component.Endpoints;

public class DataSeedImportEndpointTests
{
    private static CadsBridgeWebAppFactory CreateFactory(bool dataSeedingEnabled) =>
        new(new Dictionary<string, string?> { ["DataSeedingImportEnabled"] = dataSeedingEnabled.ToString() });

    [Fact]
    public async Task GetDataSeedImport_WhenDisabled_Returns200WithDisabledMessage()
    {
        await using var factory = CreateFactory(dataSeedingEnabled: false);

        var response = await TriggerImportJob(factory);

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        var body = await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken);
        body.Should().Contain("Data seeding import is disabled.");
    }

    [Fact]
    public async Task GetDataSeedImport_WhenEnabledAndNoFiles_Returns200WithNoFilesMessage()
    {
        await using var factory = CreateFactory(dataSeedingEnabled: true);
        factory.DataSeedFileLoaderMock
            .Setup(x => x.GetFiles())
            .Returns([]);

        var response = await TriggerImportJob(factory);

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        var body = await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken);
        body.Should().Contain("No data seed files found.");
    }

    [Fact]
    public async Task GetDataSeedImport_WhenEnabledAndFilesExist_Returns200WithFileCount()
    {
        await using var factory = CreateFactory(dataSeedingEnabled: true);
        factory.DataSeedFileLoaderMock
            .Setup(x => x.GetFiles())
            .Returns([
                new DataSeedFileDetail("001_seed.sql", "sql/v1/001_seed.sql"),
                new DataSeedFileDetail("002_seed.sql", "sql/v1/002_seed.sql")
            ]);

        var response = await TriggerImportJob(factory);

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        var body = await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken);
        using var doc = JsonDocument.Parse(body);

        doc.RootElement.GetProperty("fileCount").GetInt32().Should().Be(2);

        var fileNames = doc.RootElement.GetProperty("files")
            .EnumerateArray()
            .Select(e => e.GetString())
            .ToList();

        fileNames.Should().BeEquivalentTo(["001_seed.sql", "002_seed.sql"]);
    }

    [Fact]
    public async Task GetDataSeedImport_WhenFilesExist_WritesJobsToChannel()
    {
        await using var factory = CreateFactory(dataSeedingEnabled: true);
        factory.DataSeedFileLoaderMock
            .Setup(x => x.GetFiles())
            .Returns([new DataSeedFileDetail("001_seed.sql", "sql/v1/001_seed.sql")]);

        _ = await TriggerImportJob(factory);

        using var scope = factory.Services.CreateScope();
        var channel = scope.ServiceProvider
            .GetRequiredService<Channel<DataSeedFileLoadJob>>();

        channel.Reader.TryRead(out var job).Should().BeTrue();
        job!.FileName.Should().Be("sql/v1/001_seed.sql");
        job.TargetKey.Should().Be("data-seed/001_seed.sql");
    }

    private static async Task<HttpResponseMessage> TriggerImportJob(CadsBridgeWebAppFactory factory)
    {
        var client = factory.CreateClient().AddBasicTestApiKey();
        var response = await client.GetAsync("data-seed/import", TestContext.Current.CancellationToken);

        return response;
    }
}