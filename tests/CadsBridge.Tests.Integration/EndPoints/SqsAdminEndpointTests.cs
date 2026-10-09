using System.Net;
using Amazon.SQS;
using Amazon.SQS.Model;
using CadsBridge.Endpoints.SqsAdmin.Responses;
using CadsBridge.Testing.Support.ApiClients;
using CadsBridge.Testing.Support.Factories.Authorization;
using CadsBridge.Testing.Support.TestFixtures.Containers;
using FluentAssertions;
using System.Text.Json;

namespace CadsBridge.Tests.Integration.EndPoints;

[Collection("CadsBridgeIntegration"), Trait("Dependence", "testcontainers")]
public class SqsAdminEndpointTests(ApiContainerFixture apiContainerFixture)
{
    private const string QueueName = "cads-bridge-admin-queue";

    private IAmazonSQS Sqs => apiContainerFixture.LocalStackFixture.SqsClient;
    private string MainQueueUrl => apiContainerFixture.LocalStackFixture.CadsStandardQueueUrl!;
    private string DlqUrl => apiContainerFixture.LocalStackFixture.CadsStandardDeadLetterQueueUrl!;

    // GetQueues

    [Fact]
    public async Task GivenValidRoleAndScope_WhenGetQueuesRequested_ShouldReturnConfiguredQueue()
    {
        var client = await apiContainerFixture.CreateAzureAdClientAsync(TestSqsAdminExecuteTokenFactory.ValidUserToken());

        var response = await SqsAdminTestClient.GetQueuesAsync(client, TestContext.Current.CancellationToken);

        response.IsSuccessStatusCode.Should().BeTrue();

        var dto = await SqsAdminTestClient.ReadQueuesDtoAsync(response, TestContext.Current.CancellationToken);

        dto.Should().NotBeNull();

        var queue = dto!.Queues.Should().ContainSingle(q => q.Name == QueueName).Subject;

        // Decisive check: does the app's resolved DLQ URL actually match the DLQ
        // the test will send messages to? (QueueUrl match alone doesn't prove this.)
        queue.QueueUrl.Should().Be(MainQueueUrl);
        queue.DlqQueueUrl.Should().Be(DlqUrl);
    }

    [Fact]
    public async Task GivenScopeMissing_WhenGetQueuesRequested_ShouldReturnForbidden()
    {
        var client = await apiContainerFixture.CreateAzureAdClientAsync(TestSqsAdminExecuteTokenFactory.MissingScopeToken());

        var response = await SqsAdminTestClient.GetQueuesAsync(client, TestContext.Current.CancellationToken);

        response.StatusCode.Should().Be(HttpStatusCode.Forbidden);
    }

    // GetMetrics

    [Fact]
    public async Task GivenValidRoleAndScope_WhenGetMetricsRequested_ShouldReturnOk()
    {
        var client = await apiContainerFixture.CreateAzureAdClientAsync(TestSqsAdminExecuteTokenFactory.ValidUserToken());

        var response = await SqsAdminTestClient.GetMetricsAsync(client, QueueName, TestContext.Current.CancellationToken);

        response.IsSuccessStatusCode.Should().BeTrue();

        var dto = await SqsAdminTestClient.ReadMetricsDtoAsync(response, TestContext.Current.CancellationToken);

        dto.Should().NotBeNull();
        dto!.Queue.Should().Be(QueueName);
    }

    [Fact]
    public async Task GivenUnknownQueue_WhenGetMetricsRequested_ShouldReturnNotFound()
    {
        var client = await apiContainerFixture.CreateAzureAdClientAsync(TestSqsAdminExecuteTokenFactory.ValidUserToken());

        var response = await SqsAdminTestClient.GetMetricsAsync(client, "unknown-queue", TestContext.Current.CancellationToken);

        response.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    // GetDlqMetrics

    [Fact]
    public async Task GivenMessageOnDlq_WhenGetDlqMetricsRequested_ShouldReturnDlqMetricsAndLeaveMainMetricsUnchanged()
    {
        var client = await apiContainerFixture.CreateAzureAdClientAsync(TestSqsAdminExecuteTokenFactory.ValidUserToken());

        await DrainQueueAsync(MainQueueUrl);
        await DrainQueueAsync(DlqUrl);

        await Sqs.SendMessageAsync(new SendMessageRequest
        {
            QueueUrl = DlqUrl,
            MessageBody = JsonSerializer.Serialize(new { test = Guid.NewGuid().ToString("N") })
        }, TestContext.Current.CancellationToken);

        var dlqDto = await PollUntilAsync(
            async () =>
            {
                var response = await SqsAdminTestClient.GetDlqMetricsAsync(client, QueueName, TestContext.Current.CancellationToken);
                response.IsSuccessStatusCode.Should().BeTrue();
                return await SqsAdminTestClient.ReadMetricsDtoAsync(response, TestContext.Current.CancellationToken);
            },
            result => result?.Metrics.ApproximateNumberOfMessages >= 1);

        dlqDto.Should().NotBeNull();
        dlqDto!.Metrics.ApproximateNumberOfMessages.Should().BeGreaterThanOrEqualTo(1);

        var mainResponse = await SqsAdminTestClient.GetMetricsAsync(client, QueueName, TestContext.Current.CancellationToken);
        mainResponse.StatusCode.Should().Be(HttpStatusCode.OK);
        var mainDto = await SqsAdminTestClient.ReadMetricsDtoAsync(mainResponse, TestContext.Current.CancellationToken);
        mainDto!.Metrics.ApproximateNumberOfMessages.Should().Be(0);

        await DrainQueueAsync(DlqUrl);
    }

    [Fact]
    public async Task GivenUnknownQueue_WhenGetDlqMetricsRequested_ShouldReturnNotFound()
    {
        var client = await apiContainerFixture.CreateAzureAdClientAsync(TestSqsAdminExecuteTokenFactory.ValidUserToken());

        var response = await SqsAdminTestClient.GetDlqMetricsAsync(client, "unknown-queue", TestContext.Current.CancellationToken);

        response.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    // GetDlqMessages

    [Fact]
    public async Task GivenMessageOnDlq_WhenGetDlqMessagesRequested_ShouldReturnMessageWithoutRemovingIt()
    {
        var client = await apiContainerFixture.CreateAzureAdClientAsync(TestSqsAdminExecuteTokenFactory.ValidUserToken());

        var dedupId = Guid.NewGuid().ToString("N");
        await Sqs.SendMessageAsync(new SendMessageRequest
        {
            QueueUrl = DlqUrl,
            MessageBody = JsonSerializer.Serialize(new { test = dedupId })
        }, TestContext.Current.CancellationToken);

        var dto = await PollUntilAsync(
            async () =>
            {
                var response = await SqsAdminTestClient.GetDlqMessagesAsync(client, QueueName, maxMessages: 10, TestContext.Current.CancellationToken);
                response.IsSuccessStatusCode.Should().BeTrue();
                return await SqsAdminTestClient.ReadMessagesDtoAsync(response, TestContext.Current.CancellationToken);
            },
            result => result?.Messages.Any(m => m.Body.Contains(dedupId)) == true);

        dto.Should().NotBeNull();
        dto!.Messages.Should().Contain(m => m.Body.Contains(dedupId));

        // Peek must not consume the message - confirm it is still on the DLQ via attributes.
        var attributes = await Sqs.GetQueueAttributesAsync(new GetQueueAttributesRequest
        {
            QueueUrl = DlqUrl,
            AttributeNames = ["ApproximateNumberOfMessages"]
        }, TestContext.Current.CancellationToken);

        attributes.ApproximateNumberOfMessages.Should().BeGreaterThanOrEqualTo(1);

        await DrainQueueAsync(DlqUrl);
    }

    [Fact]
    public async Task GivenUnknownQueue_WhenGetDlqMessagesRequested_ShouldReturnNotFound()
    {
        var client = await apiContainerFixture.CreateAzureAdClientAsync(TestSqsAdminExecuteTokenFactory.ValidUserToken());

        var response = await SqsAdminTestClient.GetDlqMessagesAsync(client, "unknown-queue", maxMessages: null, TestContext.Current.CancellationToken);

        response.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    // GetMessages

    [Fact]
    public async Task GivenMessageOnQueue_WhenGetMessagesRequested_ShouldReturnMessageWithoutRemovingIt()
    {
        var client = await apiContainerFixture.CreateAzureAdClientAsync(TestSqsAdminExecuteTokenFactory.ValidUserToken());

        var dedupId = Guid.NewGuid().ToString("N");
        await Sqs.SendMessageAsync(new SendMessageRequest
        {
            QueueUrl = MainQueueUrl,
            MessageBody = JsonSerializer.Serialize(new { test = dedupId })
        }, TestContext.Current.CancellationToken);

        var dto = await PollUntilAsync(
            async () =>
            {
                var response = await SqsAdminTestClient.GetMessagesAsync(client, QueueName, maxMessages: 10, TestContext.Current.CancellationToken);
                response.IsSuccessStatusCode.Should().BeTrue();
                return await SqsAdminTestClient.ReadMessagesDtoAsync(response, TestContext.Current.CancellationToken);
            },
            result => result?.Messages.Any(m => m.Body.Contains(dedupId)) == true);

        dto.Should().NotBeNull();
        dto!.Messages.Should().Contain(m => m.Body.Contains(dedupId));

        // Peek must not consume the message - verify it's still receivable via a second call.
        var secondDto = await PollUntilAsync(
            async () =>
            {
                var response = await SqsAdminTestClient.GetMessagesAsync(client, QueueName, maxMessages: 10, TestContext.Current.CancellationToken);
                return await SqsAdminTestClient.ReadMessagesDtoAsync(response, TestContext.Current.CancellationToken);
            },
            result => result?.Messages.Any(m => m.Body.Contains(dedupId)) == true);

        secondDto!.Messages.Should().Contain(m => m.Body.Contains(dedupId));

        // Additional ground-truth check that doesn't depend on ReceiveMessage/VisibilityTimeout
        // semantics at all: confirm the message is still sitting on the queue via attributes.
        var attributes = await Sqs.GetQueueAttributesAsync(new GetQueueAttributesRequest
        {
            QueueUrl = MainQueueUrl,
            AttributeNames = ["ApproximateNumberOfMessages"]
        }, TestContext.Current.CancellationToken);

        attributes.ApproximateNumberOfMessages.Should().BeGreaterThanOrEqualTo(1);

        // Cleanup: consume the message so it doesn't bleed into other tests.
        await DrainQueueAsync(MainQueueUrl);
    }

    [Fact]
    public async Task GivenUnknownQueue_WhenGetMessagesRequested_ShouldReturnNotFound()
    {
        var client = await apiContainerFixture.CreateAzureAdClientAsync(TestSqsAdminExecuteTokenFactory.ValidUserToken());

        var response = await SqsAdminTestClient.GetMessagesAsync(client, "unknown-queue", maxMessages: null, TestContext.Current.CancellationToken);

        response.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    // ReplayDlq

    [Fact]
    public async Task GivenMessageOnDlq_WhenReplayRequested_ShouldMoveMessageToMainQueue()
    {
        var client = await apiContainerFixture.CreateAzureAdClientAsync(TestSqsAdminExecuteTokenFactory.ValidUserToken());

        var dedupId = Guid.NewGuid().ToString("N");

        await Sqs.SendMessageAsync(new SendMessageRequest
        {
            QueueUrl = DlqUrl,
            MessageBody = JsonSerializer.Serialize(new { test = dedupId })
        }, TestContext.Current.CancellationToken);


        ReplayDlqResponse? dto = null;

        await PollUntilAsync(
            async () =>
            {
                var response = await SqsAdminTestClient.ReplayDlqAsync(client, QueueName, batchSize: 1, TestContext.Current.CancellationToken);

                if (!response.IsSuccessStatusCode)
                {
                    var body = await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken);
                    throw new InvalidOperationException(
                        $"Replay request failed with status {(int)response.StatusCode} ({response.StatusCode}): {body}");
                }

                dto = await SqsAdminTestClient.ReadReplayDtoAsync(response, TestContext.Current.CancellationToken);
                return dto?.Moved >= 1;
            },
            moved => moved);

        dto.Should().NotBeNull();
        dto!.Moved.Should().BeGreaterThanOrEqualTo(1);
        dto.Failed.Should().Be(0);

        var receive = await PollUntilAsync(
            async () => await Sqs.ReceiveMessageAsync(new ReceiveMessageRequest
            {
                QueueUrl = MainQueueUrl,
                MaxNumberOfMessages = 10,
                MessageAttributeNames = ["All"]
            }, TestContext.Current.CancellationToken),
            result => (result.Messages ?? []).Any(m => m.Body.Contains(dedupId)));

        (receive.Messages ?? []).Should().Contain(m => m.Body.Contains(dedupId));

        await DrainQueueAsync(MainQueueUrl);
    }

    [Fact]
    public async Task GivenInvalidBatchSize_WhenReplayRequested_ShouldReturnBadRequest()
    {
        var client = await apiContainerFixture.CreateAzureAdClientAsync(TestSqsAdminExecuteTokenFactory.ValidUserToken());

        var response = await SqsAdminTestClient.ReplayDlqAsync(client, QueueName, batchSize: 999, TestContext.Current.CancellationToken);

        response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
    }

    [Fact]
    public async Task GivenUnknownQueue_WhenReplayRequested_ShouldReturnNotFound()
    {
        var client = await apiContainerFixture.CreateAzureAdClientAsync(TestSqsAdminExecuteTokenFactory.ValidUserToken());

        var response = await SqsAdminTestClient.ReplayDlqAsync(client, "unknown-queue", batchSize: 1, TestContext.Current.CancellationToken);

        response.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    private async Task DrainQueueAsync(string queueUrl)
    {
        while (true)
        {
            var receive = await Sqs.ReceiveMessageAsync(new ReceiveMessageRequest
            {
                QueueUrl = queueUrl,
                MaxNumberOfMessages = 10,
                VisibilityTimeout = 5
            });

            // Some SQS implementations (e.g. certain LocalStack versions) return a null
            // Messages collection rather than an empty list when the queue is empty.
            var messages = receive.Messages ?? [];

            if (messages.Count == 0) break;

            foreach (var message in messages)
            {
                await Sqs.DeleteMessageAsync(queueUrl, message.ReceiptHandle);
            }
        }
    }

    /// <summary>
    /// SQS (including LocalStack) does not guarantee that a message is immediately
    /// receivable after SendMessageAsync returns. Polls the given operation until the
    /// predicate passes or attempts are exhausted, returning the last observed result.
    /// </summary>
    private static async Task<T> PollUntilAsync<T>(
        Func<Task<T>> operation,
        Func<T, bool> isComplete,
        int maxAttempts = 10,
        int delayMilliseconds = 500)
    {
        T result = default!;

        for (var attempt = 0; attempt < maxAttempts; attempt++)
        {
            result = await operation();

            if (isComplete(result))
            {
                return result;
            }

            if (attempt < maxAttempts - 1)
            {
                await Task.Delay(delayMilliseconds);
            }
        }

        return result;
    }
}