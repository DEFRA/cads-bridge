using Amazon.S3.Model;
using CadsBridge.Application.Messaging.Messages;
using CadsBridge.Application.Messaging.Models;
using CadsBridge.Infrastructure.Messaging.Factories;
using CadsBridge.Testing.Support.Constants;
using CadsBridge.Testing.Support.TestFixtures.Containers;
using CadsBridge.Testing.Support.Utilities.Assertions;
using CadsBridge.Testing.Support.Utilities.Aws;
using FluentAssertions;

namespace CadsBridge.Tests.Integration.Consumers;

[Collection("CadsBridgeIntegration"), Trait("Dependence", "testcontainers")]
public class CadsBridgeFifoQueuePollerTests(ApiContainerFixture apiContainerFixture)
{
    public string QueueUrl => apiContainerFixture.LocalStackFixture.CadsBridgeFifoQueueUrl!;

    private readonly MessageFactory _messageFactory = new();

    private const string Salt = "test-salt";
    private const string TestDerivedValue = "2026-07-10_MYTABLE_BATCH1_FULL_TEST_CADS_CTSM";
    private const string OracleEnvironment = "Prod";
    private const string FileNameWithoutFileType = "CTSM_CADS_TEST_FULL_BATCH1_MYTABLE_2026-07-10-120000";

    [Fact]
    public async Task GivenASingleFile_WhenProcessMessageAsync_ShouldSucceed()
    {
        // Arrange
        var incomingObjectKey = $"incoming/{FileNameWithoutFileType}.csv";
        var importedObjectKey = $"import/{FileNameWithoutFileType}.csv";
        var etag = Guid.NewGuid().ToString("N");

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
        var correlationId = Guid.NewGuid().ToString();

        var messageGroupId = FifoKeyGenerator.GenerateMessageGroupId(
            incomingObjectKey,
            OracleEnvironment);

        var messageDeduplicationId = FifoKeyGenerator.GenerateDeduplicationId(
            TestS3Constants.TestCadsBridgeExternalBucketName,
            incomingObjectKey,
            etag,
            OracleEnvironment);

        var message = GetCsvDataFileImportMessage(
            TestS3Constants.TestCadsBridgeExternalBucketName,
            incomingObjectKey,
            OracleEnvironment,
            etag,
            DateTime.UtcNow,
            correlationId);

        var metadata = new FifoMessageMetadata(
            messageGroupId,
            messageDeduplicationId,
            correlationId);

        await ExecuteQueueTest(message, metadata);

        // Assert
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

    private async Task ExecuteQueueTest<TMessage>(TMessage message, FifoMessageMetadata metadata)
    {
        var request = _messageFactory.CreateFifoSqsMessage(
            QueueUrl,
            message,
            metadata);

        using var cts = new CancellationTokenSource();
        await apiContainerFixture.LocalStackFixture.SqsClient.SendMessageAsync(
            request,
            cts.Token);
    }

    private static CsvDataFileImportMessage GetCsvDataFileImportMessage(
        string bucket,
        string objectKey,
        string oracleEnvironment,
        string etag,
        DateTime discoveredAtUtc,
        string correlationId) => new()
        {
            Bucket = bucket,
            ObjectKey = objectKey,
            OracleEnvironment = oracleEnvironment,
            Etag = etag,
            DiscoveredAtUtc = discoveredAtUtc,
            CorrelationId = correlationId
        };
}