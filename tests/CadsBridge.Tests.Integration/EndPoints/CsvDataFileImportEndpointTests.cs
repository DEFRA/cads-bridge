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
public class CsvDataFileImportEndpointTests
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

    [Fact]
    public async Task ImportFile_WithOneFile_DecryptsAndSplitsFile()
    {
        // Arrange
        const string fileNameWithoutFileType = "test-file";
        const string incomingTestFileCsv = "incoming/test-file.csv";
        const string importedTestFileCsv = "import/test-file.csv";
        const string password = "pwd1";
        const string salt = "adfsb8123";
        await using var fixture = new ApiContainerFixture();
        await fixture.InitializeAsync();

        var fileContents = string.Join(
            Environment.NewLine,
            "HEADER|ignored",
            "C|RECORD_TYPE|COLUMN_ONE|COLUMN_TWO",
            "D|1|One",
            "D|2|Two",
            "D|3|Three") + Environment.NewLine;

        using var encryptedStream = await fileContents.Encrypt(password, salt, TestContext.Current.CancellationToken);
        await fixture.LocalStackFixture.S3Client.PutObjectAsync(new PutObjectRequest
        {
            BucketName = TestS3Constants.TestCadsBridgeExternalBucketName,
            Key = incomingTestFileCsv,
            InputStream = encryptedStream
        }, TestContext.Current.CancellationToken);

        // Act
        var jobId = await TriggerImportJob(fixture, new CsvDataFileImportRequest([
            new CsvDataFileImportRequestItem(sourceKey: incomingTestFileCsv)
        ]));

        // Assert
        await AsyncAssert.WaitForAssertion(async () =>
            {
                var status = await GetImportJobStatus(fixture, jobId);
                status!.JobId.Should().Be(jobId);
                status.TotalFiles.Should().Be(1);
                status.Files.First().Status.Should().Be(JobStatus.Succeeded);
            },
            backOffMilliSeconds: 500,
            attempts: 20);

        await AsyncAssert.WaitForAssertion(async () =>
            {
                var listObjectsV2Response = await fixture.LocalStackFixture.S3Client.ListObjectsV2Async(
                    new ListObjectsV2Request()
                    {
                        BucketName = TestS3Constants.TestCadsBridgeInternalBucketName,
                        Prefix = $"import/{fileNameWithoutFileType}"
                    },
                    TestContext.Current.CancellationToken);
                listObjectsV2Response.S3Objects.Should().HaveCount(2);
                listObjectsV2Response.S3Objects[0].Key.Should().Be($"import/{fileNameWithoutFileType}/{fileNameWithoutFileType}-part-0001.csv");
                listObjectsV2Response.S3Objects[1].Key.Should().Be($"import/{fileNameWithoutFileType}/{fileNameWithoutFileType}-part-0002.csv");
            });

        // The original imported file should always remain alongside the split parts.
        await AsyncAssert.WaitForAssertion(async () =>
            {
                var importedFile = await fixture.LocalStackFixture.S3Client.ListObjectsV2Async(
                    new ListObjectsV2Request()
                    {
                        BucketName = TestS3Constants.TestCadsBridgeInternalBucketName,
                        Prefix = importedTestFileCsv
                    },
                    TestContext.Current.CancellationToken);
                importedFile.S3Objects.Should().ContainSingle(o => o.Key == importedTestFileCsv);
            });
    }

    [Fact]
    public async Task ImportFile_WithOneFile_AndSplitTypeNone_CopiesWholeFileAsSinglePart()
    {
        // Arrange
        const string fileNameWithoutFileType = "test-file-none";
        const string incomingTestFileCsv = "incoming/test-file-none.csv";
        const string password = "pwd1";
        const string salt = "adfsb8123";
        await using var fixture = new ApiContainerFixture();
        await fixture.InitializeAsync();

        var fileContents = string.Join(
            Environment.NewLine,
            "HEADER|ignored",
            "C|RECORD_TYPE|COLUMN_ONE|COLUMN_TWO",
            "D|1|One") + Environment.NewLine;

        using var encryptedStream = await fileContents.Encrypt(password, salt, TestContext.Current.CancellationToken);
        await fixture.LocalStackFixture.S3Client.PutObjectAsync(new PutObjectRequest
        {
            BucketName = TestS3Constants.TestCadsBridgeExternalBucketName,
            Key = incomingTestFileCsv,
            InputStream = encryptedStream
        }, TestContext.Current.CancellationToken);

        // Act
        var jobId = await TriggerImportJob(fixture, new CsvDataFileImportRequest([
            new CsvDataFileImportRequestItem(sourceKey: incomingTestFileCsv)
        ]));

        // Assert
        await AsyncAssert.WaitForAssertion(async () =>
            {
                var status = await GetImportJobStatus(fixture, jobId);
                status!.JobId.Should().Be(jobId);
                status.TotalFiles.Should().Be(1);
                status.Files.First().Status.Should().Be(JobStatus.Succeeded);
            },
            backOffMilliSeconds: 500,
            attempts: 20);

        await AsyncAssert.WaitForAssertion(async () =>
            {
                var listObjectsV2Response = await fixture.LocalStackFixture.S3Client.ListObjectsV2Async(
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