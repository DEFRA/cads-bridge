using System.Net;
using System.Net.Http.Json;
using CadsBridge.Application.Models;
using CadsBridge.Application.Persistance;
using CadsBridge.Testing.Support.TestFixtures.Containers;
using CadsBridge.Testing.Support.Utilities.Http;
using FluentAssertions;

namespace CadsBridge.Tests.Integration.EndPoints;

[Collection("CadsBridgeIntegration"), Trait("Dependence", "testcontainers")]
public class FileImportEndpointTests
{
    [Fact]
    public async Task ImportFile_WithNoFiles_CreatesAnImportJobWithNoFiles()
    {
        await using var fixture = new ApiContainerFixture();
        await fixture.InitializeAsync();

        var jobId = await TriggerImportJob(fixture);

        jobId.Should().NotBeNullOrEmpty();

        var status = await GetImportJobStatus(fixture, jobId);

        status!.JobId.Should().Be(jobId);
        status.TotalFiles.Should().Be(0);
        status.CompletedFiles.Should().Be(0);
        status.Files.Should().BeEmpty();
    }

    private sealed record ImportJobResponse(string JobId);
    private static async Task<string> TriggerImportJob(ApiContainerFixture fixture)
    {
        var response = await fixture.HttpClient!.PostAsync("import", EmptyImportRequest, TestContext.Current.CancellationToken);

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        var result = await response.Content.ReadFromJsonAsync<ImportJobResponse>(cancellationToken: TestContext.Current.CancellationToken);
        return result!.JobId;
    }

    private static async Task<JobProgress?> GetImportJobStatus(ApiContainerFixture fixture, string jobId)
    {
        var response = await fixture.HttpClient!.GetAsync($"import/{jobId}/progress", TestContext.Current.CancellationToken);

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        return await response.Content.ReadFromJsonAsync<JobProgress>(TestContext.Current.CancellationToken);
    }

    private static StringContent? EmptyImportRequest =>
        HttpContentUtility.CreateApplicationJsonAsStringContent(new ImportRequest(new List<ImportRequestItem>()));

}