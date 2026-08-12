using Amazon.S3.Model;
using Amazon.SQS.Model;
using CadsBridge.Application.Messaging.Messages;
using CadsBridge.Infrastructure.Json;
using CadsBridge.Testing.Support.Constants;
using CadsBridge.Testing.Support.TestFixtures.Containers;
using CadsBridge.Testing.Support.Utilities.Assertions;
using FluentAssertions;
using System.Text.Json;

namespace CadsBridge.Tests.Integration.Jobs;

/// <summary>
/// End-to-end integration test for <see cref="CadsBridge.Worker.Jobs.BulkScanJob"/>.
/// Expected outcome: exactly 2 SQS messages are enqueued (files 4 and 5).
/// </summary>
[Trait("Dependence", "testcontainers")]
public class BulkScanJobTests
{
    private const string CompleteFile = "CTSM_CADS_PROD_BULK_ABC_0004_CT_PARTIES_2026-01-01-012345.csv";
    private const string FailedFile = "CTSM_CADS_PROD_BULK_ABC_0005_CT_PARTIES_2026-01-01-012345.csv";
    private const string InvalidFilename = "invalid-filename.csv";
    private const string DeltaTypeFile = "CTSM_CADS_PROD_DELTA_XYZ_0001_CT_ANIMALS_2026-07-31-120000.csv";
    private const string NewFile = "CTSM_CADS_PROD_BULK_NEW_0001_CT_ANIMALS_2026-07-31-120000.csv";
    private const string Prefix = "cads/cts/bulk/";

    [Fact]
    public async Task BulkScanJob_HappyPath_EnqueuesOnlyValidFilesNotYetCompleted()
    {
        // ── Arrange: start the container with the fake CDS API and the queue consumer
        //   disabled so BulkScanJob messages stay in the queue long enough for the test
        //   to read them
        await using var fixture = new ApiContainerWithEnvsFixture(new Dictionary<string, string>
        {
            ["Messaging__DisableQueueConsumer"] = "true",
            ["Quartz__Jobs__0__Enabled"] = "true",
            ["Quartz__Jobs__0__CronSchedule"] = "*/10 * * * * ?"
        });

        await fixture.InitializeAsync();

        var s3 = fixture.LocalStackFixture.S3Client;
        var sqs = fixture.LocalStackFixture.SqsClient;
        var externalBucket = TestS3Constants.TestCadsBridgeExternalBucketName;
        var queueUrl = fixture.LocalStackFixture.CadsBridgeFifoQueueUrl!;
        var ct = TestContext.Current.CancellationToken;

        // Upload all five test objects to the external bucket.
        foreach (var key in new[] { InvalidFilename, DeltaTypeFile, CompleteFile, FailedFile, NewFile })
        {
            await s3.PutObjectAsync(new PutObjectRequest
            {
                BucketName = externalBucket,
                Key = Prefix + key,
                ContentBody = "test"
            }, ct);
        }

        // ── Assert: wait for the Quartz job to fire and enqueue the expected messages ──
        var receivedMessages = new List<CsvDataFileImportMessage>();

        await AsyncAssert.WaitForAssertion(async () =>
        {
            var response = await sqs.ReceiveMessageAsync(new ReceiveMessageRequest
            {
                QueueUrl = queueUrl,
                MaxNumberOfMessages = 10,
                WaitTimeSeconds = 0,
                MessageAttributeNames = ["All"],
                MessageSystemAttributeNames = ["All"]
            }, ct);

            foreach (var sqsMessage in response.Messages)
            {
                var deserialized = JsonSerializer.Deserialize<CsvDataFileImportMessage>(
                    sqsMessage.Body,
                    JsonDefaults.DefaultOptionsWithStringEnumConversion);

                if (deserialized is not null &&
                    !receivedMessages.Any(m => m.ObjectKey == deserialized.ObjectKey))
                {
                    receivedMessages.Add(deserialized);
                }
            }

            receivedMessages.Should().HaveCount(2);
        }, backOffMilliSeconds: 5000, attempts: 10);

        // ── Verify the two expected files were enqueued ──
        var enqueuedKeys = receivedMessages.Select(m => m.ObjectKey).ToList();

        enqueuedKeys.Should().Contain(Prefix + FailedFile,
            because: "a Failed file with 0 prior attempts must be re-enqueued");

        enqueuedKeys.Should().Contain(Prefix + NewFile,
            because: "a file with no prior import record must be enqueued for the first time");

        // ── Verify the files that must NOT be enqueued ──
        enqueuedKeys.Should().NotContain(Prefix + CompleteFile,
            because: "a Completed file must be skipped by IsFileValid");

        enqueuedKeys.Should().NotContain(Prefix + InvalidFilename,
            because: "an invalid CTSM filename must be dropped before any CDS API call");

        enqueuedKeys.Should().NotContain(Prefix + DeltaTypeFile,
            because: "a non-BULK type file must be dropped by GetValidBulkFileNames");

        // ── Verify the bucket and correlation metadata are present ──
        receivedMessages.Should().AllSatisfy(m =>
        {
            m.Bucket.Should().Be(externalBucket);
            m.CorrelationId.Should().NotBeNullOrWhiteSpace();
            m.Etag.Should().NotBeNullOrWhiteSpace();
            m.DiscoveredAtUtc.Should().BeCloseTo(DateTime.UtcNow, precision: TimeSpan.FromMinutes(2));
        });
    }
}