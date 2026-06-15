using System.Net;
using System.Net.Http.Json;
using Amazon.S3.Model;
using CadsBridge.Application.Models;
using CadsBridge.Application.Persistance;
using CadsBridge.Testing.Support.Constants;
using CadsBridge.Testing.Support.TestFixtures.Containers;
using CadsBridge.Testing.Support.Utilities.Assertions;
using CadsBridge.Testing.Support.Utilities.Aws;
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

    [Fact]
    public async Task ImportFile_WithOneFile_DecryptsAndSplitsFile()
    {
        // Arrange
        const string fileNameWithoutFileType = "test-file";
        const string incomingTestFileCsv = "incoming/test-file.csv";
        const string importedTestFileCsv = "imported/test-file.csv";
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
        var jobId = await TriggerImportJob(fixture, new ImportRequest([
            new ImportRequestItem(
                JobId: string.Empty,
                sourceKey: incomingTestFileCsv,
                targetKey: importedTestFileCsv,
                Password: password,
                Salt: salt,
                SplitType: SplitType.ByLines,
                SplitValue: 2)
        ]));

        // Assert
        await AsyncAssert.WaitForAssertion(async () =>
            {
                var status = await GetImportJobStatus(fixture, jobId);
                status!.JobId.Should().Be(jobId);
                status.TotalFiles.Should().Be(1);
                status.Files.First().Status.Should().Be(JobStatus.Succeeded);
            });

        await AsyncAssert.WaitForAssertion(async () =>
            {
                var listObjectsV2Response = await fixture.LocalStackFixture.S3Client.ListObjectsV2Async(
                    new ListObjectsV2Request()
                    {
                        BucketName = TestS3Constants.TestCadsBridgeInternalBucketName,
                        Prefix = fileNameWithoutFileType
                    },
                    TestContext.Current.CancellationToken);
                listObjectsV2Response.S3Objects.Should().HaveCount(2);
                listObjectsV2Response.S3Objects[0].Key.Should().Be($"{fileNameWithoutFileType}/{fileNameWithoutFileType}.part-0001.csv");
                listObjectsV2Response.S3Objects[1].Key.Should().Be($"{fileNameWithoutFileType}/{fileNameWithoutFileType}.part-0002.csv");
            });
    }

    private sealed record ImportJobResponse(string JobId);
    private static async Task<string> TriggerImportJob(ApiContainerFixture fixture, ImportRequest? request = null)
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
        HttpContentUtility.CreateApplicationJsonAsStringContent(new ImportRequest(new List<ImportRequestItem>()));

}