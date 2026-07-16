using Amazon.S3.Model;
using CadsBridge.Endpoints.Requests;
using CadsBridge.Testing.Support.Constants;
using CadsBridge.Testing.Support.TestFixtures.Containers;
using CadsBridge.Testing.Support.Utilities.Assertions;
using CadsBridge.Testing.Support.Utilities.Aws;
using CadsBridge.Testing.Support.Utilities.Http;
using FluentAssertions;
using System.Net;

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
        var response = await TriggerImportJob(apiContainerFixture);

        response.StatusCode.Should().Be(HttpStatusCode.OK);
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
            "D|4|Four",
            "D|5|Five",
            "D|6|Six",
            "D|7|Seven",
            "D|8|Eight",
            "D|9|Nine",
            "D|10|Ten",
            "D|11|Eleven",
            $"T|{FileNameWithoutFileType}.csv|01012000 00:00:00|3") + Environment.NewLine;

        using var encryptedStream = await fileContents.Encrypt(TestDerivedValue, Salt, TestContext.Current.CancellationToken);
        await apiContainerFixture.LocalStackFixture.S3Client.PutObjectAsync(new PutObjectRequest
        {
            BucketName = TestS3Constants.TestCadsBridgeExternalBucketName,
            Key = incomingObjectKey,
            InputStream = encryptedStream
        }, TestContext.Current.CancellationToken);

        // Act
        var response = await TriggerImportJob(apiContainerFixture, new CsvDataFileImportRequest([
            new CsvDataFileImportRequestItem(incomingObjectKey)
        ]));

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.OK);

        await AsyncAssert.WaitForAssertion(async () =>
        {
            var listObjectsV2Response = await apiContainerFixture.LocalStackFixture.S3Client.ListObjectsV2Async(
                new ListObjectsV2Request()
                {
                    BucketName = TestS3Constants.TestCadsBridgeInternalBucketName,
                    Prefix = $"import/{FileNameWithoutFileType}"
                },
                TestContext.Current.CancellationToken);
            listObjectsV2Response.S3Objects.Should().HaveCount(4);

            // The original imported file should always remain alongside the split parts.
            listObjectsV2Response.S3Objects.Where(x => x.Key == $"import/{FileNameWithoutFileType}.csv").Should().NotBeNull();

            // The parts from the imported file (SplitValue set as 5 so expect 3 parts)
            listObjectsV2Response.S3Objects.Where(x => x.Key == $"import/{FileNameWithoutFileType}/{FileNameWithoutFileType}-part-0001.csv").Should().NotBeNull();
            listObjectsV2Response.S3Objects.Where(x => x.Key == $"import/{FileNameWithoutFileType}/{FileNameWithoutFileType}-part-0002.csv").Should().NotBeNull();
            listObjectsV2Response.S3Objects.Where(x => x.Key == $"import/{FileNameWithoutFileType}/{FileNameWithoutFileType}-part-0003.csv").Should().NotBeNull();
        });
    }

    private sealed record ImportJobResponse(string JobId);

    private static async Task<HttpResponseMessage> TriggerImportJob(ApiContainerFixture fixture, CsvDataFileImportRequest? request = null)
    {
        var requestContent = request != null ? HttpContentUtility.CreateApplicationJsonAsStringContent(request) : EmptyImportRequest;

        var response = await fixture.HttpClient!.PostAsync("import", requestContent, TestContext.Current.CancellationToken);

        return response;
    }

    private static StringContent? EmptyImportRequest =>
        HttpContentUtility.CreateApplicationJsonAsStringContent(new CsvDataFileImportRequest([]));
}