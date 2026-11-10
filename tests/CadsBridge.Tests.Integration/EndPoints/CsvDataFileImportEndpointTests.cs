using Amazon.S3.Model;
using CadsBridge.Endpoints.Requests;
using CadsBridge.Infrastructure.Encryption;
using CadsBridge.Infrastructure.Encryption.Providers;
using CadsBridge.Testing.Support.Constants;
using CadsBridge.Testing.Support.TestFixtures.Containers;
using CadsBridge.Testing.Support.Utilities.Assertions;
using CadsBridge.Testing.Support.Utilities.Aws;
using CadsBridge.Testing.Support.Utilities.Encryption;
using CadsBridge.Testing.Support.Utilities.Http;
using FluentAssertions;
using System.Net;
using System.Text;

namespace CadsBridge.Tests.Integration.Endpoints;

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
        var incomingObjectKey = $"cads/cts/bulk/{FileNameWithoutFileType}.csv";
        var importedObjectKey = $"import/cts/bulk/{FileNameWithoutFileType}.csv";

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
            $"T|{FileNameWithoutFileType}.csv|01012000 00:00:00|11") + Environment.NewLine;

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
                    Prefix = $"import/cts/bulk/{FileNameWithoutFileType}"
                },
                TestContext.Current.CancellationToken);
            listObjectsV2Response.S3Objects.Should().HaveCount(4);

            // The original imported file should always remain alongside the split parts.
            listObjectsV2Response.S3Objects.Where(x => x.Key == $"import/cts/bulk/{FileNameWithoutFileType}.csv").Should().NotBeNull();

            // The parts from the imported file (SplitValue set as 5 so expect 3 parts)
            listObjectsV2Response.S3Objects.Where(x => x.Key == $"import/cts/bulk/{FileNameWithoutFileType}/{FileNameWithoutFileType}-part-0001.csv").Should().NotBeNull();
            listObjectsV2Response.S3Objects.Where(x => x.Key == $"import/cts/bulk/{FileNameWithoutFileType}/{FileNameWithoutFileType}-part-0002.csv").Should().NotBeNull();
            listObjectsV2Response.S3Objects.Where(x => x.Key == $"import/cts/bulk/{FileNameWithoutFileType}/{FileNameWithoutFileType}-part-0003.csv").Should().NotBeNull();
        });

        var decryptedPayload1 = await DownloadAndDecryptAsync($"import/cts/bulk/{FileNameWithoutFileType}/{FileNameWithoutFileType}-part-0001.csv");

        decryptedPayload1.Should().Contain("record_type|column_one|column_two");
        decryptedPayload1.Should().Contain("D|1|One");
        decryptedPayload1.Should().Contain("D|2|Two");
        decryptedPayload1.Should().Contain("D|3|Three");
        decryptedPayload1.Should().Contain("D|4|Four");
        decryptedPayload1.Should().Contain("D|5|Five");

        var decryptedPayload2 = await DownloadAndDecryptAsync($"import/cts/bulk/{FileNameWithoutFileType}/{FileNameWithoutFileType}-part-0002.csv");

        decryptedPayload2.Should().Contain("record_type|column_one|column_two");
        decryptedPayload2.Should().Contain("D|6|Six");
        decryptedPayload2.Should().Contain("D|7|Seven");
        decryptedPayload2.Should().Contain("D|8|Eight");
        decryptedPayload2.Should().Contain("D|9|Nine");
        decryptedPayload2.Should().Contain("D|10|Ten");

        var decryptedPayload3 = await DownloadAndDecryptAsync($"import/cts/bulk/{FileNameWithoutFileType}/{FileNameWithoutFileType}-part-0003.csv");

        decryptedPayload3.Should().Contain("record_type|column_one|column_two");
        decryptedPayload3.Should().Contain("D|11|Eleven");
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

    private async Task<string> DownloadAndDecryptAsync(string key)
    {
        using var s3Object = await apiContainerFixture.LocalStackFixture.S3Client
            .GetObjectAsync(
                TestS3Constants.TestCadsBridgeInternalBucketName,
                key,
                TestContext.Current.CancellationToken);

        await using var encryptedStream = new MemoryStream();
        await s3Object.ResponseStream.CopyToAsync(
            encryptedStream,
            TestContext.Current.CancellationToken);

        var encryptedBytes = encryptedStream.ToArray();

        encryptedBytes.Should().NotBeEmpty();

        encryptedStream.Position = 0;

        await using var decryptedStream = new MemoryStream();

        var encryptor = new FileEncryptor(
            new InMemoryMasterKeyProvider(
                TestEncryptionKeyProvider.CurrentKeyId,
                TestEncryptionKeyProvider.Keys));

        await encryptor.DecryptAsync(
            encryptedStream,
            decryptedStream,
            cancellationToken: TestContext.Current.CancellationToken);

        return Encoding.UTF8.GetString(decryptedStream.ToArray());
    }

}