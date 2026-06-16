using System.Net;
using System.Net.Http.Json;
using CadsBridge.Application.Models;
using CadsBridge.Application.Persistance;
using CadsBridge.Infrastructure.FileSplit;
using CadsBridge.Testing.Support.Constants;
using CadsBridge.Testing.Support.Utilities.Assertions;
using CadsBridge.Testing.Support.Utilities.Aws;
using CadsBridge.Testing.Support.Utilities.Http;
using CadsBridge.Tests.Component.TestFixtures;
using FluentAssertions;
using Moq;

namespace CadsBridge.Tests.Component.EndPoints;

public class FileImportEndpointTests
{
    private string _testPassword = "test-password";
    private string _testSalt = "test-salt";
    private string _incomingKey = "incoming/test-file.txt";
    private string _importedKey = "imported/test-file.txt";
    private string _fileNameWithoutTypeSuffix = "test-file";

    [Fact]
    public async Task ImportFile_WithNoFiles_CreatesAnImportJobWithNoFiles()
    {
        await using var factory = new CadsBridgeWebAppFactory();
        var client = factory.CreateClient();

        var jobId = await TriggerImportJob(client);

        var status = await GetImportJobStatus(jobId, client);
        status!.JobId.Should().Be(jobId);
        status.TotalFiles.Should().Be(0);
        status.CompletedFiles.Should().Be(0);
        status.Files.Should().BeEmpty();
    }

    [Fact]
    public async Task ImportFile_WithOneFileNotFoundInS3_CreatesAnImportJobWithOneFileAndFails()
    {
        await using var factory = new CadsBridgeWebAppFactory(null, false);
        var client = factory.CreateClient();

        var request = new ImportRequest([
            new ImportRequestItem(
                JobId: string.Empty,
                sourceKey: _incomingKey,
                targetKey: _importedKey,
                Password: _testPassword,
                Salt: _testSalt,
                SplitType: SplitType.None,
                SplitValue: null)
        ]);

        var jobId = await TriggerImportJob(client, request);

        await AsyncAssert.WaitForAssertion(async () =>
        {
            var status = await GetImportJobStatus(jobId, client);
            status!.JobId.Should().Be(jobId);
            status.TotalFiles.Should().Be(1);
            status.Files.First().Status.Should().Be(JobStatus.Failed);
        });
    }

    [Fact]
    public async Task ImportFile_WithOneFile_CreatesAnImportJobWithOneFile_DecryptsAndSplitsFile()
    {
        await using var factory = new CadsBridgeWebAppFactory(null, false);
        var fileSplitterMock = new Mock<ISplitMessageProducer>();
        factory.OverrideSingleton(fileSplitterMock.Object);
        await factory.AmazonS3Mock.SetUpEncryptedFileAsync(TestS3Constants.TestCadsBridgeExternalBucketName, _incomingKey, _testPassword, _testSalt, TestContext.Current.CancellationToken);
        var client = factory.CreateClient();

        var jobId = await TriggerImportJob(client, new ImportRequest([
            new ImportRequestItem(
                JobId: string.Empty,
                sourceKey: _incomingKey,
                targetKey: _importedKey,
                Password: _testPassword,
                Salt: _testSalt,
                SplitType: SplitType.ByLines,
                SplitValue: 1)
        ]));

        var expectedFileSplitJob = new FileSplitJob(jobId, _importedKey, _fileNameWithoutTypeSuffix, SplitType.ByLines, 1);
        await fileSplitterMock.AsyncVerify(x => x.SendAsync(expectedFileSplitJob, It.IsAny<CancellationToken>()), Times.Once);
        var status = await GetImportJobStatus(jobId, client);
        status!.JobId.Should().Be(jobId);
        status.TotalFiles.Should().Be(1);
        status.Files.First().Status.Should().Be(JobStatus.Succeeded);
    }

    private sealed record ImportJobResponse(string JobId);
    private static async Task<string> TriggerImportJob(HttpClient httpClient, ImportRequest? request = null)
    {
        var content = HttpContentUtility.CreateApplicationJsonAsStringContent(request ?? new ImportRequest([]));

        var response = await httpClient.PostAsync("import", content, TestContext.Current.CancellationToken);

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        var result = await response.Content.ReadFromJsonAsync<ImportJobResponse>(cancellationToken: TestContext.Current.CancellationToken);
        var jobId = result!.JobId;
        jobId.Should().NotBeNullOrEmpty();
        return jobId;
    }

    private static async Task<JobProgress?> GetImportJobStatus(string jobId, HttpClient httpClient)
    {
        var response = await httpClient.GetAsync($"import/{jobId}/progress", TestContext.Current.CancellationToken);

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        return await response.Content.ReadFromJsonAsync<JobProgress>(TestContext.Current.CancellationToken);
    }
}