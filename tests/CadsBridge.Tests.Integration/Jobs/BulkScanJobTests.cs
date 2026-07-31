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
///
/// The BulkScanJob fires every second (Quartz cron "* * * * * ?").
/// Five objects are uploaded to the external S3 bucket covering every filtering branch:
///
///   1. <c>invalid-filename.csv</c>            – fails CtsmFilenameParser → dropped at filename validation
///   2. <c>CTSM_CADS_PROD_DELTA_*</c>          – type=DELTA, not BULK     → dropped by GetValidBulkFileNames
///   3. <c>CTSM_CADS_PROD_BULK_ABC_0004_*</c>  – seeded Complete (4)      → IsFileValid=false → ignored
///   4. <c>CTSM_CADS_PROD_BULK_ABC_0005_*</c>  – seeded Failed (5), 0 attempts → IsFileValid=true → enqueued
///   5. <c>CTSM_CADS_PROD_BULK_NEW_0001_*</c>  – not in fake list (null)  → IsFileValid=true → enqueued
///
/// Expected outcome: exactly 2 SQS messages are enqueued (files 4 and 5).
/// </summary>
[Trait("Dependence", "testcontainers")]
public class BulkScanJobTests
{
    // ── File names matching 0003_026 seed data rows (no .csv — S3 key == DB file_name) ──
    // These must stay in sync with the hardcoded entries in FakeFileImportApiService.
    private const string CompleteFile = "CTSM_CADS_PROD_BULK_ABC_0004_CT_PARTIES_2026-01-01-012345";
    private const string FailedFile = "CTSM_CADS_PROD_BULK_ABC_0005_CT_PARTIES_2026-01-01-012345";

    // ── Additional files to exercise the other filter branches ──
    private const string InvalidFilename = "invalid-filename.csv";
    private const string DeltaTypeFile = "CTSM_CADS_PROD_DELTA_XYZ_0001_CT_ANIMALS_2026-07-31-120000";
    private const string NewFile = "CTSM_CADS_PROD_BULK_NEW_0001_CT_ANIMALS_2026-07-31-120000";

    [Fact]
    public async Task BulkScanJob_HappyPath_EnqueuesOnlyValidFilesNotYetCompleted()
    {
        // ── Arrange: start the container with the fake CDS API and the queue consumer
        //   disabled so BulkScanJob messages stay in the queue long enough for the test
        //   to read them (otherwise the poller races to drain the queue first). ──
        await using var fixture = new ApiContainerWithEnvsFixture(new Dictionary<string, string>
        {
            ["Messaging__DisableQueueConsumer"] = "true",
        });

        await fixture.InitializeAsync();

        var s3 = fixture.LocalStackFixture.S3Client;
        var sqs = fixture.LocalStackFixture.SqsClient;
        var externalBucket = TestS3Constants.TestCadsBridgeExternalBucketName;
        var queueUrl = fixture.LocalStackFixture.CadsBridgeFifoQueueUrl!;
        var ct = TestContext.Current.CancellationToken;

        // Upload all five test objects to the external bucket.
        // Content is not inspected during the bulk scan — a single byte is sufficient.
        foreach (var key in new[] { InvalidFilename, DeltaTypeFile, CompleteFile, FailedFile, NewFile })
        {
            await s3.PutObjectAsync(new PutObjectRequest
            {
                BucketName = externalBucket,
                Key = key,
                ContentBody = "test"
            }, ct);
        }

        // ── Assert: wait for the Quartz job to fire and enqueue the expected messages ──
        // The job fires every second; we poll up to 30 times (×500 ms = ~15 s total) to
        // give the container time to start up, run the first qualifying job cycle, and
        // publish both messages.  Messages are accumulated across poll attempts so that
        // a single slow response does not cause a false failure.
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
        }, backOffMilliSeconds: 500, attempts: 30);

        // ── Verify the two expected files were enqueued ──
        var enqueuedKeys = receivedMessages.Select(m => m.ObjectKey).ToList();

        enqueuedKeys.Should().Contain(FailedFile,
            because: "a Failed file with 0 prior attempts must be re-enqueued");

        enqueuedKeys.Should().Contain(NewFile,
            because: "a file with no prior import record must be enqueued for the first time");

        // ── Verify the files that must NOT be enqueued ──
        enqueuedKeys.Should().NotContain(CompleteFile,
            because: "a Completed file must be skipped by IsFileValid");

        enqueuedKeys.Should().NotContain(InvalidFilename,
            because: "an invalid CTSM filename must be dropped before any CDS API call");

        enqueuedKeys.Should().NotContain(DeltaTypeFile,
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