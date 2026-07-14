using CadsBridge.Application.DataLoad.Jobs;
using CadsBridge.Application.DataLoad.Messaging;
using CadsBridge.Core.DataLoad.Jobs;
using CadsBridge.Core.Exceptions;
using CadsBridge.Endpoints.Requests;
using CadsBridge.Testing.Support.Constants;
using CadsBridge.Testing.Support.Utilities.Assertions;
using CadsBridge.Testing.Support.Utilities.Aws;
using CadsBridge.Testing.Support.Utilities.Http;
using CadsBridge.Tests.Component.TestFixtures;
using FluentAssertions;
using Moq;
using System.Net;
using System.Net.Http.Json;

namespace CadsBridge.Tests.Component.Endpoints;

public class CsvDataFileImportEndpointTests
{
    // CTSM-format filename: CTSM_<app>_<env>_<type>_<batchId>_<tablename>_<timestamp>.csv
    // Password is derived from the filename by CtsmFilenameParser as "<app>_<env>_<type>_<batchId>".
    private const string CtsmFilename = "CTSM_CADS_TEST_FULL_BATCH1_MYTABLE_2026-07-10-120000.csv";
    private readonly string _testDerivedValue = "2026-07-10_MYTABLE_BATCH1_FULL_TEST_CADS_CTSM";
    private readonly string _testSalt = "test-salt";
    private readonly string _incomingKey = $"incoming/{CtsmFilename}";
    private readonly string _importedKey = $"import/{CtsmFilename}";

    private static Dictionary<string, string?> SaltOverride(string salt) => new() { ["DataLoad:Salt"] = salt };

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

        var request = new CsvDataFileImportRequest([
            new CsvDataFileImportRequestItem(sourceKey: _incomingKey)
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
        await using var factory = new CadsBridgeWebAppFactory(SaltOverride(_testSalt), false);
        var fileSplitterMock = new Mock<ISplitMessageProducer>();
        factory.OverrideSingleton(fileSplitterMock.Object);
        await factory.AmazonS3Mock.SetUpEncryptedFileAsync(TestS3Constants.TestCadsBridgeExternalBucketName, _incomingKey, _testDerivedValue, _testSalt, TestContext.Current.CancellationToken);
        var client = factory.CreateClient();

        var jobId = await TriggerImportJob(client, new CsvDataFileImportRequest([
            new CsvDataFileImportRequestItem(sourceKey: _incomingKey)
        ]));

        var expectedFileSplitJob = new CsvDataFileSplitJob(jobId, _importedKey, FileImportStatusId: 1L);
        await fileSplitterMock.AsyncVerify(x => x.SendAsync(expectedFileSplitJob, It.IsAny<CancellationToken>()), Times.Once);
        var status = await GetImportJobStatus(jobId, client);
        status!.JobId.Should().Be(jobId);
        status.TotalFiles.Should().Be(1);
        status.Files.First().Status.Should().Be(JobStatus.Succeeded);
    }

    [Fact]
    public async Task ImportFile_WithOneFile_CallsS3MetaDataServiceAndFileImportStatusStore_WithExpectedArguments()
    {
        await using var factory = new CadsBridgeWebAppFactory(null, false);
        var fileSplitterMock = new Mock<ISplitMessageProducer>();
        factory.OverrideSingleton(fileSplitterMock.Object);
        factory.S3FileMetaDataServiceMock
            .Setup(x => x.GetRecordCountAsync(_incomingKey, It.IsAny<CancellationToken>()))
            .ReturnsAsync(555L);
        factory.FileImportStatusStoreMock
            .Setup(x => x.Initiate(_incomingKey, 555L, It.IsAny<CancellationToken>()))
            .ReturnsAsync(9L);
        await factory.AmazonS3Mock.SetUpEncryptedFileAsync(TestS3Constants.TestCadsBridgeExternalBucketName, _incomingKey, _testDerivedValue, _testSalt, TestContext.Current.CancellationToken);
        var client = factory.CreateClient();

        await TriggerImportJob(client, new CsvDataFileImportRequest([
            new CsvDataFileImportRequestItem(sourceKey: _incomingKey)
        ]));

        factory.S3FileMetaDataServiceMock.Verify(
            x => x.GetRecordCountAsync(_incomingKey, It.IsAny<CancellationToken>()), Times.Once);
        factory.FileImportStatusStoreMock.Verify(
            x => x.Initiate(_incomingKey, 555L, It.IsAny<CancellationToken>()), Times.Once);
        factory.FileImportStatusStoreMock.Verify(
            x => x.MarkInProgress(9L, It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task ImportFile_WhenS3MetaDataServiceThrowsNotFoundException_MarksFileAsFailedButReturnsOk()
    {
        await using var factory = new CadsBridgeWebAppFactory(null, false);
        var notFoundMessage = $"S3 object '{_incomingKey}' was not found.";
        factory.S3FileMetaDataServiceMock
            .Setup(x => x.GetRecordCountAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new NotFoundException(notFoundMessage));
        var client = factory.CreateClient();

        var jobId = await TriggerImportJob(client, new CsvDataFileImportRequest([
            new CsvDataFileImportRequestItem(sourceKey: _incomingKey)
        ]));

        await AsyncAssert.WaitForAssertion(async () =>
        {
            var status = await GetImportJobStatus(jobId, client);
            status!.JobId.Should().Be(jobId);
            status.TotalFiles.Should().Be(1);
            status.CompletedFiles.Should().Be(1);
            status.Files.First().Status.Should().Be(JobStatus.Failed);
            status.Files.First().ErrorMessage.Should().Be(notFoundMessage);
        });

        factory.FileImportStatusStoreMock.Verify(
            x => x.Initiate(It.IsAny<string>(), It.IsAny<long>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task ImportFile_WhenFileImportStatusStoreInitiateThrows_MarksFileAsFailedButReturnsOk()
    {
        await using var factory = new CadsBridgeWebAppFactory(null, false);
        const string errorMessage = "downstream unavailable";
        factory.FileImportStatusStoreMock
            .Setup(x => x.Initiate(It.IsAny<string>(), It.IsAny<long>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException(errorMessage));
        var client = factory.CreateClient();

        var jobId = await TriggerImportJob(client, new CsvDataFileImportRequest([
            new CsvDataFileImportRequestItem(sourceKey: _incomingKey)
        ]));

        await AsyncAssert.WaitForAssertion(async () =>
        {
            var status = await GetImportJobStatus(jobId, client);
            status!.JobId.Should().Be(jobId);
            status.TotalFiles.Should().Be(1);
            status.CompletedFiles.Should().Be(1);
            status.Files.First().Status.Should().Be(JobStatus.Failed);
            status.Files.First().ErrorMessage.Should().Be(errorMessage);
        });

        factory.FileImportStatusStoreMock.Verify(
            x => x.MarkInProgress(It.IsAny<long>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task ImportFile_WhenCopySucceeds_MarksFileImportStatusInProgress()
    {
        await using var factory = new CadsBridgeWebAppFactory(SaltOverride(_testSalt), false);
        var fileSplitterMock = new Mock<ISplitMessageProducer>();
        factory.OverrideSingleton(fileSplitterMock.Object);
        factory.FileImportStatusStoreMock
            .Setup(x => x.Initiate(It.IsAny<string>(), It.IsAny<long>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(42L);
        await factory.AmazonS3Mock.SetUpEncryptedFileAsync(TestS3Constants.TestCadsBridgeExternalBucketName, _incomingKey, _testDerivedValue, _testSalt, TestContext.Current.CancellationToken);
        var client = factory.CreateClient();

        var jobId = await TriggerImportJob(client, new CsvDataFileImportRequest([
            new CsvDataFileImportRequestItem(sourceKey: _incomingKey)
        ]));

        await AsyncAssert.WaitForAssertion(async () =>
        {
            var status = await GetImportJobStatus(jobId, client);
            status!.Files.First().Status.Should().Be(JobStatus.Succeeded);
        });

        // The split step (which marks the file import status as succeeded) only runs via the
        // real CsvDataFileSplitBackgroundService; since ISplitMessageProducer is mocked out here,
        // only the import stage's audit call (MarkInProgress) is expected.
        factory.FileImportStatusStoreMock.Verify(x => x.MarkInProgress(42L, It.IsAny<CancellationToken>()), Times.Once);
        factory.FileImportStatusStoreMock.Verify(x => x.MarkSucceeded(It.IsAny<long>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    private sealed record ImportJobResponse(string JobId);
    private static async Task<string> TriggerImportJob(HttpClient httpClient, CsvDataFileImportRequest? request = null)
    {
        var content = HttpContentUtility.CreateApplicationJsonAsStringContent(request ?? new CsvDataFileImportRequest([]));

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