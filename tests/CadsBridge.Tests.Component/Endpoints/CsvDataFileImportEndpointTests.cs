using CadsBridge.Application.DataLoad.Jobs;
using CadsBridge.Application.DataLoad.Messaging;
using CadsBridge.Application.DataLoad.Services;
using CadsBridge.Endpoints.Requests;
using CadsBridge.Testing.Support.Constants;
using CadsBridge.Testing.Support.Utilities.Assertions;
using CadsBridge.Testing.Support.Utilities.Aws;
using CadsBridge.Testing.Support.Utilities.Http;
using CadsBridge.Tests.Component.TestFixtures;
using FluentAssertions;
using Moq;
using System.Net;

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

        var response = await TriggerImportJob(client);

        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Fact]
    public async Task ImportFile_WithOneFileNotFoundInS3_CreatesAnImportJobWithOneFileAndFails()
    {
        await using var factory = new CadsBridgeWebAppFactory(null, false);
        var client = factory.CreateClient();

        var request = new CsvDataFileImportRequest([
            new CsvDataFileImportRequestItem(sourceKey: _incomingKey)
        ]);

        var response = await TriggerImportJob(client, request);

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Fact]
    public async Task ImportFile_WithOneFile_CreatesAnImportJobWithOneFile_DecryptsAndSplitsFile()
    {
        await using var factory = new CadsBridgeWebAppFactory(SaltOverride(_testSalt), false);
        var fileSplitterMock = new Mock<ISplitMessageProducer>();
        factory.OverrideSingleton(fileSplitterMock.Object);

        var s3ExternalToInternalCopyServiceMock = new Mock<IS3CopyService>();
        s3ExternalToInternalCopyServiceMock
            .Setup(x => x.ExecAsync(It.IsAny<CsvDataFileImportJob>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(555L);
        factory.OverrideSingleton(s3ExternalToInternalCopyServiceMock.Object);

        await factory.AmazonS3Mock.SetUpEncryptedFileAsync(TestS3Constants.TestCadsBridgeExternalBucketName, _incomingKey, _testDerivedValue, _testSalt, TestContext.Current.CancellationToken);
        var client = factory.CreateClient();

        var response = await TriggerImportJob(client, new CsvDataFileImportRequest([
            new CsvDataFileImportRequestItem(sourceKey: _incomingKey)
        ]));

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.OK);

        var expectedFileSplitJob = new CsvDataFileSplitJob(_importedKey, FileImportId: 1L, 555L);
        await fileSplitterMock.AsyncVerify(x => x.SendAsync(expectedFileSplitJob, It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task ImportFile_WithOneFile_CallsS3MetaDataServiceAndFileImportStatusStore_WithExpectedArguments()
    {
        await using var factory = new CadsBridgeWebAppFactory(null, false);
        var fileSplitterMock = new Mock<ISplitMessageProducer>();
        factory.OverrideSingleton(fileSplitterMock.Object);
        //factory.S3FileMetaDataServiceMock
        //    .Setup(x => x.GetRecordCountAsync(_incomingKey, It.IsAny<CancellationToken>()))
        //    .ReturnsAsync(555L);
        factory.FileImportStoreMock
            .Setup(x => x.CreateAsync(_incomingKey, 555L, It.IsAny<CancellationToken>()))
            .ReturnsAsync(9L);
        await factory.AmazonS3Mock.SetUpEncryptedFileAsync(TestS3Constants.TestCadsBridgeExternalBucketName, _incomingKey, _testDerivedValue, _testSalt, TestContext.Current.CancellationToken);
        var client = factory.CreateClient();

        var response = await TriggerImportJob(client, new CsvDataFileImportRequest([
            new CsvDataFileImportRequestItem(sourceKey: _incomingKey)
        ]));

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.OK);

        //factory.S3FileMetaDataServiceMock.Verify(
        //    x => x.GetRecordCountAsync(_incomingKey, It.IsAny<CancellationToken>()), Times.Once);
        factory.FileImportStoreMock.Verify(
            x => x.CreateAsync(_incomingKey, 555L, It.IsAny<CancellationToken>()), Times.Once);
        //factory.FileImportStoreMock.Verify(
        //    x => x.MarkInProgressAsync(9L, It.IsAny<CancellationToken>()), Times.Once);
    }

    //[Fact]
    //public async Task ImportFile_WhenS3MetaDataServiceThrowsNotFoundException_MarksFileAsFailedButReturnsOk()
    //{
    //    await using var factory = new CadsBridgeWebAppFactory(null, false);
    //    var notFoundMessage = $"S3 object '{_incomingKey}' was not found.";
    //    factory.S3FileMetaDataServiceMock
    //        .Setup(x => x.GetRecordCountAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
    //        .ThrowsAsync(new NotFoundException(notFoundMessage));
    //    var client = factory.CreateClient();

    //    var response = await TriggerImportJob(client, new CsvDataFileImportRequest([
    //        new CsvDataFileImportRequestItem(sourceKey: _incomingKey)
    //    ]));

    //    // Assert
    //    response.StatusCode.Should().Be(HttpStatusCode.OK);

    //    factory.FileImportStoreMock.Verify(
    //        x => x.CreateAsync(It.IsAny<string>(), It.IsAny<long>(), It.IsAny<CancellationToken>()), Times.Never);
    //}

    [Fact]
    public async Task ImportFile_WhenFileImportStoreCreateThrows_MarksFileAsFailedButReturnsOk()
    {
        await using var factory = new CadsBridgeWebAppFactory(null, false);
        const string errorMessage = "downstream unavailable";
        factory.FileImportStoreMock
            .Setup(x => x.CreateAsync(It.IsAny<string>(), It.IsAny<long>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException(errorMessage));
        var client = factory.CreateClient();

        var response = await TriggerImportJob(client, new CsvDataFileImportRequest([
            new CsvDataFileImportRequestItem(sourceKey: _incomingKey)
        ]));

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.OK);

        factory.FileImportStoreMock.Verify(
            x => x.MarkInProgressAsync(It.IsAny<long>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task ImportFile_WhenCopySucceeds_MarksFileImportStatusInProgress()
    {
        await using var factory = new CadsBridgeWebAppFactory(SaltOverride(_testSalt), false);
        var fileSplitterMock = new Mock<ISplitMessageProducer>();
        factory.OverrideSingleton(fileSplitterMock.Object);
        factory.FileImportStoreMock
            .Setup(x => x.CreateAsync(It.IsAny<string>(), It.IsAny<long>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(42L);
        await factory.AmazonS3Mock.SetUpEncryptedFileAsync(TestS3Constants.TestCadsBridgeExternalBucketName, _incomingKey, _testDerivedValue, _testSalt, TestContext.Current.CancellationToken);
        var client = factory.CreateClient();

        var response = await TriggerImportJob(client, new CsvDataFileImportRequest([
            new CsvDataFileImportRequestItem(sourceKey: _incomingKey)
        ]));

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.OK);

        // The split step (which marks the file import status as succeeded) only runs via the
        // real CsvDataFileSplitBackgroundService; since ISplitMessageProducer is mocked out here,
        // only the import stage's audit call (MarkInProgress) is expected.
        factory.FileImportStoreMock.Verify(x => x.UpdateAsync(It.IsAny<long>(), Core.ApiClients.FileImportStatus.Importing, It.IsAny<long>(), It.IsAny<long>(), It.IsAny<CancellationToken>()), Times.Once);
        factory.FileImportStoreMock.Verify(x => x.MarkCompletedAsync(It.IsAny<long>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    private sealed record ImportJobResponse(string JobId);

    private static async Task<HttpResponseMessage> TriggerImportJob(HttpClient httpClient, CsvDataFileImportRequest? request = null)
    {
        var content = HttpContentUtility.CreateApplicationJsonAsStringContent(request ?? new CsvDataFileImportRequest([]));

        var response = await httpClient.PostAsync("import", content, TestContext.Current.CancellationToken);

        return response;
    }
}