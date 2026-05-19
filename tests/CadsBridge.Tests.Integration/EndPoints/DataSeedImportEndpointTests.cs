using System.Net;
using CadsBridge.Testing.Support.TestFixtures.Containers;
using FluentAssertions;

namespace CadsBridge.Tests.Infrastructure.Endpoints;

[Collection("CadsBridgeIntegration"), Trait("Dependence", "testcontainers")]
public class DataSeedImportEndpointTests
{
    [Fact]
    public async Task GetDataSeedImport_WhenDisabled_Returns200WithDisabledMessage()
    {
        await using var fixture = new ApiContainerFixture(new Dictionary<string, string>
        {
            ["DataSeedingImportEnabled"] = "false"
        });
        await fixture.InitializeAsync();

        var response = await fixture.HttpClient.GetAsync("data-seed/import", TestContext.Current.CancellationToken);

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        var body = await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken);
        body.Should().Contain("Data seeding import is disabled.");
    }

    [Fact]
    public async Task GetDataSeedImport_WhenEnabledAndNoSqlFiles_Returns200WithNoFilesMessage()
    {
        await using var fixture = new ApiContainerFixture(new Dictionary<string, string>
        {
            ["DataSeedingImportEnabled"] = "true"
        });
        await fixture.InitializeAsync();

        var response = await fixture.HttpClient.GetAsync("data-seed/import", TestContext.Current.CancellationToken);

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        var body = await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken);
        body.Should().Contain("No data seed files found.");
    }
}