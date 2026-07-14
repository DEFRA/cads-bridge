using Amazon.S3.Model;
using CadsBridge.Core.DataLoad.Jobs;
using CadsBridge.Endpoints.Requests;
using CadsBridge.Testing.Support.Constants;
using CadsBridge.Testing.Support.TestFixtures.Containers;
using CadsBridge.Testing.Support.Utilities.Assertions;
using CadsBridge.Testing.Support.Utilities.Aws;
using CadsBridge.Testing.Support.Utilities.Http;
using FluentAssertions;
using System.Net;
using System.Net.Http.Json;

namespace CadsBridge.Tests.Integration.EndPoints;

[Collection("CadsBridgeIntegration"), Trait("Dependence", "testcontainers")]
public class CsvDataFileImportEndpointTests(ApiContainerFixture apiContainerFixture)
{
    private const string Salt = "test-salt";
    private const string TestDerivedValue = "2026-07-10_MYTABLE_BATCH1_FULL_TEST_CADS_CTSM";
    private const string FileNameWithoutFileType = "CTSM_CADS_TEST_FULL_BATCH1_MYTABLE_2026-07-10-120000";

    [Fact]
    public async Task ImportFile_WithNoFiles_CreatesAnImportJobWithNoFiles()
    {
        var jobId = await TriggerImportJob(apiContainerFixture);
        jobId.Should().NotBeNullOrEmpty();

        var status = await GetImportJobStatus(apiContainerFixture, jobId);

        status!.JobId.Should().Be(jobId);
        status.TotalFiles.Should().Be(0);
        status.CompletedFiles.Should().Be(0);
        status.Files.Should().BeEmpty();
    }

    [Fact]
    public async Task ImportFile_WithOneFile_DecryptsAndSplitsFile()
    {
        // Arrange
        var incomingObjectKey = $"incoming/{FileNameWithoutFileType}.csv";
        var importedObjectKey = $"import/{FileNameWithoutFileType}.csv";

        var fileContents = string.Join(
            Environment.NewLine,
            "HEADER|ignored",
            "C|RECORD_TYPE|COLUMN_ONE|COLUMN_TWO",
            "D|1|One",
            "D|2|Two",
            "D|3|Three",
            $"T|{FileNameWithoutFileType}.csv|01012000 00:00:00|3") + Environment.NewLine;

        using var encryptedStream = await fileContents.Encrypt(TestDerivedValue, Salt, TestContext.Current.CancellationToken);
        await apiContainerFixture.LocalStackFixture.S3Client.PutObjectAsync(new PutObjectRequest
        {
            BucketName = TestS3Constants.TestCadsBridgeExternalBucketName,
            Key = incomingObjectKey,
            InputStream = encryptedStream
        }, TestContext.Current.CancellationToken);

        // Act
        var jobId = await TriggerImportJob(apiContainerFixture, new CsvDataFileImportRequest([
            new CsvDataFileImportRequestItem(incomingObjectKey)
        ]));

        // Assert
        await AsyncAssert.WaitForAssertion(async () =>
            {
                var status = await GetImportJobStatus(apiContainerFixture, jobId);
                status!.JobId.Should().Be(jobId);
                status.TotalFiles.Should().Be(1);
                status.Files.First().Status.Should().Be(JobStatus.Succeeded);
            },
            backOffMilliSeconds: 500,
            attempts: 10);

        await AsyncAssert.WaitForAssertion(async () =>
            {
                var listObjectsV2Response = await apiContainerFixture.LocalStackFixture.S3Client.ListObjectsV2Async(
                    new ListObjectsV2Request()
                    {
                        BucketName = TestS3Constants.TestCadsBridgeInternalBucketName,
                        Prefix = $"import/{FileNameWithoutFileType}"
                    },
                    TestContext.Current.CancellationToken);
                listObjectsV2Response.S3Objects.Should().HaveCount(2);
                listObjectsV2Response.S3Objects[0].Key.Should().Be($"import/{FileNameWithoutFileType}/{FileNameWithoutFileType}-part-0001.csv");
                listObjectsV2Response.S3Objects[1].Key.Should().Be($"import/{FileNameWithoutFileType}/{FileNameWithoutFileType}-part-0002.csv");
            });

        // The original imported file should always remain alongside the split parts.
        await AsyncAssert.WaitForAssertion(async () =>
            {
                var importedFile = await apiContainerFixture.LocalStackFixture.S3Client.ListObjectsV2Async(
                    new ListObjectsV2Request()
                    {
                        BucketName = TestS3Constants.TestCadsBridgeInternalBucketName,
                        Prefix = "import"
                    },
                    TestContext.Current.CancellationToken);
                importedFile.S3Objects.Should().ContainSingle(o => o.Key == importedObjectKey);
            });
    }

    [Fact]
    public async Task ImportFile_WithOneFile_AndSplitTypeNone_CopiesWholeFileAsSinglePart()
    {
        // Arrange
        const string fileNameWithoutFileType = "test-file-none";
        const string incomingTestFileCsv = "incoming/test-file-none.csv";
        const string password = "pwd1";
        const string salt = "test-salt";

        var fileContents = string.Join(
            Environment.NewLine,
            "HEADER|ignored",
            "C|RECORD_TYPE|COLUMN_ONE|COLUMN_TWO",
            "D|1|One",
            "T|test-file-none.csv|01012000 00:00:00|1") + Environment.NewLine;

        using var encryptedStream = await fileContents.Encrypt(password, salt, TestContext.Current.CancellationToken);
        await apiContainerFixture.LocalStackFixture.S3Client.PutObjectAsync(new PutObjectRequest
        {
            BucketName = TestS3Constants.TestCadsBridgeExternalBucketName,
            Key = incomingTestFileCsv,
            InputStream = encryptedStream
        }, TestContext.Current.CancellationToken);

        // Act
        var jobId = await TriggerImportJob(apiContainerFixture, new CsvDataFileImportRequest([
            new CsvDataFileImportRequestItem(sourceKey: incomingTestFileCsv)
        ]));

        // Assert
        await AsyncAssert.WaitForAssertion(async () =>
            {
                var status = await GetImportJobStatus(apiContainerFixture, jobId);
                status!.JobId.Should().Be(jobId);
                status.TotalFiles.Should().Be(1);
                status.Files.First().Status.Should().Be(JobStatus.Succeeded);
            },
            backOffMilliSeconds: 500,
            attempts: 20);

        await AsyncAssert.WaitForAssertion(async () =>
            {
                var listObjectsV2Response = await apiContainerFixture.LocalStackFixture.S3Client.ListObjectsV2Async(
                    new ListObjectsV2Request()
                    {
                        BucketName = TestS3Constants.TestCadsBridgeInternalBucketName,
                        Prefix = $"import/{fileNameWithoutFileType}"
                    },
                    TestContext.Current.CancellationToken);
                listObjectsV2Response.S3Objects.Should().ContainSingle();
                listObjectsV2Response.S3Objects[0].Key.Should().Be($"import/{fileNameWithoutFileType}/{fileNameWithoutFileType}-part-0001.csv");
            });
    }

    private sealed record ImportJobResponse(string JobId);
    private static async Task<string> TriggerImportJob(ApiContainerFixture fixture, CsvDataFileImportRequest? request = null)
    {
        var requestContent = request != null ? HttpContentUtility.CreateApplicationJsonAsStringContent(request) : EmptyImportRequest;
        var response = await fixture.HttpClient!.PostAsync("import", requestContent, TestContext.Current.CancellationToken);

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
        HttpContentUtility.CreateApplicationJsonAsStringContent(new CsvDataFileImportRequest([]));

}