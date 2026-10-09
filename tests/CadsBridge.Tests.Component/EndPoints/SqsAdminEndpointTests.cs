using System.Net;
using CadsBridge.Core.DTOs.SqsAdmin;
using CadsBridge.Core.Exceptions;
using CadsBridge.Testing.Support.ApiClients;
using CadsBridge.Testing.Support.Constants;
using CadsBridge.Testing.Support.Utilities.Authorization;
using CadsBridge.Tests.Component.TestFixtures;
using CadsBridge.Tests.Unit.TestFixtures;
using FluentAssertions;
using Moq;

namespace CadsBridge.Tests.Component.EndPoints;

public class SqsAdminEndpointTests(CadsBridgeTestFixture testFixture) : IClassFixture<CadsBridgeTestFixture>
{
    private HttpClient _httpClient => testFixture.HttpClient;

    // GetQueues

    [Fact]
    public async Task GivenQueuesConfigured_WhenGetQueuesRequested_ShouldReturnOk()
    {
        var queues = new List<QueueInfoDto>
        {
            new("CadsBridgeFifo", "https://sqs.test/queue", "https://sqs.test/queue-dlq")
        };

        testFixture.Factory.SqsAdminServiceMock
            .Setup(x => x.GetQueuesAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(queues);

        var response = await SqsAdminTestClient.GetQueuesAsync(_httpClient, TestContext.Current.CancellationToken);

        response.StatusCode.Should().Be(HttpStatusCode.OK);

        var dto = await SqsAdminTestClient.ReadQueuesDtoAsync(response, TestContext.Current.CancellationToken);
        dto.Should().NotBeNull();
        dto!.Queues.Should().ContainSingle(q => q.Name == "CadsBridgeFifo");
    }

    [Fact]
    public async Task GivenNoToken_WhenGetQueuesRequested_ShouldReturnUnauthorized()
    {
        var client = testFixture.Factory.CreateClient();

        var response = await SqsAdminTestClient.GetQueuesAsync(client, TestContext.Current.CancellationToken);

        response.StatusCode.Should().Be(HttpStatusCode.Unauthorized);
    }

    [Fact]
    public async Task GivenScopeClaimMissing_WhenGetQueuesRequested_ShouldReturnForbidden()
    {
        var client = testFixture.Factory.CreateClient();
        client.AddJwt(TestAuthConstants.FakeJwtMissingSqsAdminScope);

        var response = await SqsAdminTestClient.GetQueuesAsync(client, TestContext.Current.CancellationToken);

        response.StatusCode.Should().Be(HttpStatusCode.Forbidden);
    }

    [Fact]
    public async Task GivenSqsAdminEndpointsDisabled_WhenGetQueuesRequested_ShouldReturnNotFound()
    {
        await using var factory = new CadsBridgedWebApplicationFactory(
            configOverrides: new Dictionary<string, string?>
            {
                ["EnableAdminEndpoints"] = "false"
            },
            useFakeAuth: true);

        var client = factory.CreateClient();
        client.AddJwt();

        var response = await SqsAdminTestClient.GetQueuesAsync(client, TestContext.Current.CancellationToken);

        response.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    // GetMetrics

    [Fact]
    public async Task GivenValidQueue_WhenGetMetricsRequested_ShouldReturnOk()
    {
        var metrics = new QueueMetricsDto(3, 1, 0, 42);

        testFixture.Factory.SqsAdminServiceMock
            .Setup(x => x.GetMetricsAsync("CadsBridgeFifo", It.IsAny<CancellationToken>()))
            .ReturnsAsync(metrics);

        var response = await SqsAdminTestClient.GetMetricsAsync(_httpClient, "CadsBridgeFifo", TestContext.Current.CancellationToken);

        response.StatusCode.Should().Be(HttpStatusCode.OK);

        var dto = await SqsAdminTestClient.ReadMetricsDtoAsync(response, TestContext.Current.CancellationToken);
        dto.Should().NotBeNull();
        dto!.Queue.Should().Be("CadsBridgeFifo");
        dto.Metrics.ApproximateNumberOfMessages.Should().Be(3);
    }

    [Fact]
    public async Task GivenUnknownQueue_WhenGetMetricsRequested_ShouldReturnNotFound()
    {
        testFixture.Factory.SqsAdminServiceMock
            .Setup(x => x.GetMetricsAsync("unknown-queue", It.IsAny<CancellationToken>()))
            .ThrowsAsync(new NotFoundException("Queue", "unknown-queue"));

        var response = await SqsAdminTestClient.GetMetricsAsync(_httpClient, "unknown-queue", TestContext.Current.CancellationToken);

        response.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task GivenNoToken_WhenGetMetricsRequested_ShouldReturnUnauthorized()
    {
        var client = testFixture.Factory.CreateClient();

        var response = await SqsAdminTestClient.GetMetricsAsync(client, "CadsCds", TestContext.Current.CancellationToken);

        response.StatusCode.Should().Be(HttpStatusCode.Unauthorized);
    }

    [Fact]
    public async Task GivenValidQueue_WhenGetMetricsRequested_ShouldNotCallGetDlqMetrics()
    {
        testFixture.Factory.SqsAdminServiceMock
            .Setup(x => x.GetMetricsAsync("MainOnlyQueue", It.IsAny<CancellationToken>()))
            .ReturnsAsync(new QueueMetricsDto(3, 1, 0, 42));

        var response = await SqsAdminTestClient.GetMetricsAsync(_httpClient, "MainOnlyQueue", TestContext.Current.CancellationToken);

        response.StatusCode.Should().Be(HttpStatusCode.OK);

        testFixture.Factory.SqsAdminServiceMock.Verify(x => x.GetDlqMetricsAsync(
            "MainOnlyQueue",
            It.IsAny<CancellationToken>()), Times.Never);
    }

    // GetDlqMetrics

    [Fact]
    public async Task GivenQueueWithDlq_WhenGetDlqMetricsRequested_ShouldReturnOk()
    {
        testFixture.Factory.SqsAdminServiceMock
            .Setup(x => x.GetDlqMetricsAsync("WithDlq", It.IsAny<CancellationToken>()))
            .ReturnsAsync(new QueueMetricsDto(9, 0, 0, 100));

        var response = await SqsAdminTestClient.GetDlqMetricsAsync(_httpClient, "WithDlq", TestContext.Current.CancellationToken);

        response.StatusCode.Should().Be(HttpStatusCode.OK);

        var dto = await SqsAdminTestClient.ReadMetricsDtoAsync(response, TestContext.Current.CancellationToken);
        dto.Should().NotBeNull();
        dto!.Queue.Should().Be("WithDlq");
        dto.Metrics.ApproximateNumberOfMessages.Should().Be(9);
        dto.Metrics.OldestMessageAgeSeconds.Should().Be(100);
    }

    [Fact]
    public async Task GivenUnknownQueue_WhenGetDlqMetricsRequested_ShouldReturnNotFound()
    {
        testFixture.Factory.SqsAdminServiceMock
            .Setup(x => x.GetDlqMetricsAsync("unknown-queue", It.IsAny<CancellationToken>()))
            .ThrowsAsync(new NotFoundException("Queue", "unknown-queue"));

        var response = await SqsAdminTestClient.GetDlqMetricsAsync(_httpClient, "unknown-queue", TestContext.Current.CancellationToken);

        response.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task GivenQueueWithoutDlq_WhenGetDlqMetricsRequested_ShouldReturnUnprocessable()
    {
        testFixture.Factory.SqsAdminServiceMock
            .Setup(x => x.GetDlqMetricsAsync("NoDlq", It.IsAny<CancellationToken>()))
            .ThrowsAsync(new UnprocessableException("Queue 'NoDlq' does not have a configured DLQ."));

        var response = await SqsAdminTestClient.GetDlqMetricsAsync(_httpClient, "NoDlq", TestContext.Current.CancellationToken);

        response.StatusCode.Should().Be(HttpStatusCode.UnprocessableEntity);
    }

    [Fact]
    public async Task GivenNoToken_WhenGetDlqMetricsRequested_ShouldReturnUnauthorized()
    {
        var client = testFixture.Factory.CreateClient();

        var response = await SqsAdminTestClient.GetDlqMetricsAsync(client, "CadsCds", TestContext.Current.CancellationToken);

        response.StatusCode.Should().Be(HttpStatusCode.Unauthorized);
    }

    [Fact]
    public async Task GivenScopeClaimMissing_WhenGetDlqMetricsRequested_ShouldReturnForbidden()
    {
        var client = testFixture.Factory.CreateClient();
        client.AddJwt(TestAuthConstants.FakeJwtMissingSqsAdminScope);

        var response = await SqsAdminTestClient.GetDlqMetricsAsync(client, "CadsCds", TestContext.Current.CancellationToken);

        response.StatusCode.Should().Be(HttpStatusCode.Forbidden);
    }

    [Fact]
    public async Task GivenSqsAdminEndpointsDisabled_WhenGetDlqMetricsRequested_ShouldReturnNotFound()
    {
        await using var factory = new CadsBridgedWebApplicationFactory(
            configOverrides: new Dictionary<string, string?>
            {
                ["EnableAdminEndpoints"] = "false"
            },
            useFakeAuth: true);

        var client = factory.CreateClient();
        client.AddJwt();

        var response = await SqsAdminTestClient.GetDlqMetricsAsync(client, "CadsCds", TestContext.Current.CancellationToken);

        response.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    // GetDlqMessages

    [Fact]
    public async Task GivenValidQueue_WhenGetDlqMessagesRequested_ShouldReturnOkWithDefaultMax()
    {
        var messages = new List<QueueMessageDto>
        {
            new("dlq-1", "{\"foo\":1}", new Dictionary<string, string>(), DateTimeOffset.UtcNow)
        };

        testFixture.Factory.SqsAdminServiceMock
            .Setup(x => x.PeekDlqMessagesAsync(
                It.Is<PeekMessagesRequestDto>(r => r.Queue == "CadsBridgeFifo" && r.MaxMessages == 10),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(messages);

        var response = await SqsAdminTestClient.GetDlqMessagesAsync(_httpClient, "CadsBridgeFifo", maxMessages: null, TestContext.Current.CancellationToken);

        response.StatusCode.Should().Be(HttpStatusCode.OK);

        var dto = await SqsAdminTestClient.ReadMessagesDtoAsync(response, TestContext.Current.CancellationToken);
        dto.Should().NotBeNull();
        dto!.Messages.Should().ContainSingle(m => m.MessageId == "dlq-1");
    }

    [Fact]
    public async Task GivenMaxMessagesSupplied_WhenGetDlqMessagesRequested_ShouldPassThroughToService()
    {
        testFixture.Factory.SqsAdminServiceMock
            .Setup(x => x.PeekDlqMessagesAsync(
                It.Is<PeekMessagesRequestDto>(r => r.Queue == "CadsBridgeFifo" && r.MaxMessages == 3),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync([]);

        var response = await SqsAdminTestClient.GetDlqMessagesAsync(_httpClient, "CadsBridgeFifo", maxMessages: 3, TestContext.Current.CancellationToken);

        response.StatusCode.Should().Be(HttpStatusCode.OK);

        testFixture.Factory.SqsAdminServiceMock.Verify(x => x.PeekDlqMessagesAsync(
            It.Is<PeekMessagesRequestDto>(r => r.MaxMessages == 3),
            It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task GivenUnknownQueue_WhenGetDlqMessagesRequested_ShouldReturnNotFound()
    {
        testFixture.Factory.SqsAdminServiceMock
            .Setup(x => x.PeekDlqMessagesAsync(
                It.Is<PeekMessagesRequestDto>(r => r.Queue == "unknown-queue"),
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(new NotFoundException("Queue", "unknown-queue"));

        var response = await SqsAdminTestClient.GetDlqMessagesAsync(_httpClient, "unknown-queue", maxMessages: null, TestContext.Current.CancellationToken);

        response.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task GivenQueueWithoutDlq_WhenGetDlqMessagesRequested_ShouldReturnUnprocessable()
    {
        testFixture.Factory.SqsAdminServiceMock
            .Setup(x => x.PeekDlqMessagesAsync(
                It.Is<PeekMessagesRequestDto>(r => r.Queue == "NoDlq"),
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(new UnprocessableException("Queue 'NoDlq' does not have a configured DLQ."));

        var response = await SqsAdminTestClient.GetDlqMessagesAsync(_httpClient, "NoDlq", maxMessages: null, TestContext.Current.CancellationToken);

        response.StatusCode.Should().Be(HttpStatusCode.UnprocessableEntity);
    }

    [Fact]
    public async Task GivenNoToken_WhenGetDlqMessagesRequested_ShouldReturnUnauthorized()
    {
        var client = testFixture.Factory.CreateClient();

        var response = await SqsAdminTestClient.GetDlqMessagesAsync(client, "CadsCds", maxMessages: null, TestContext.Current.CancellationToken);

        response.StatusCode.Should().Be(HttpStatusCode.Unauthorized);
    }

    [Fact]
    public async Task GivenScopeClaimMissing_WhenGetDlqMessagesRequested_ShouldReturnForbidden()
    {
        var client = testFixture.Factory.CreateClient();
        client.AddJwt(TestAuthConstants.FakeJwtMissingSqsAdminScope);

        var response = await SqsAdminTestClient.GetDlqMessagesAsync(client, "CadsCds", maxMessages: null, TestContext.Current.CancellationToken);

        response.StatusCode.Should().Be(HttpStatusCode.Forbidden);
    }

    [Fact]
    public async Task GivenSqsAdminEndpointsDisabled_WhenGetDlqMessagesRequested_ShouldReturnNotFound()
    {
        await using var factory = new CadsBridgedWebApplicationFactory(
            configOverrides: new Dictionary<string, string?>
            {
                ["EnableAdminEndpoints"] = "false"
            },
            useFakeAuth: true);

        var client = factory.CreateClient();
        client.AddJwt();

        var response = await SqsAdminTestClient.GetDlqMessagesAsync(client, "CadsBridgeFifo", maxMessages: null, TestContext.Current.CancellationToken);

        response.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    // GetMessages

    [Fact]
    public async Task GivenValidQueue_WhenGetMessagesRequested_ShouldReturnOk()
    {
        var messages = new List<QueueMessageDto>
        {
            new("msg-1", "{\"foo\":1}", new Dictionary<string, string>(), DateTimeOffset.UtcNow)
        };

        testFixture.Factory.SqsAdminServiceMock
            .Setup(x => x.PeekMessagesAsync(
                It.Is<PeekMessagesRequestDto>(r => r.Queue == "CadsBridgeFifo" && r.MaxMessages == 10),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(messages);

        var response = await SqsAdminTestClient.GetMessagesAsync(_httpClient, "CadsBridgeFifo", maxMessages: null, TestContext.Current.CancellationToken);

        response.StatusCode.Should().Be(HttpStatusCode.OK);

        var dto = await SqsAdminTestClient.ReadMessagesDtoAsync(response, TestContext.Current.CancellationToken);
        dto.Should().NotBeNull();
        dto!.Messages.Should().ContainSingle(m => m.MessageId == "msg-1");
    }

    [Fact]
    public async Task GivenMaxMessagesSupplied_WhenGetMessagesRequested_ShouldPassThroughToService()
    {
        testFixture.Factory.SqsAdminServiceMock
            .Setup(x => x.PeekMessagesAsync(
                It.Is<PeekMessagesRequestDto>(r => r.Queue == "CadsBridgeFifo" && r.MaxMessages == 3),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync([]);

        var response = await SqsAdminTestClient.GetMessagesAsync(_httpClient, "CadsBridgeFifo", maxMessages: 3, TestContext.Current.CancellationToken);

        response.StatusCode.Should().Be(HttpStatusCode.OK);

        testFixture.Factory.SqsAdminServiceMock.Verify(x => x.PeekMessagesAsync(
            It.Is<PeekMessagesRequestDto>(r => r.MaxMessages == 3),
            It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task GivenUnknownQueue_WhenGetMessagesRequested_ShouldReturnNotFound()
    {
        testFixture.Factory.SqsAdminServiceMock
            .Setup(x => x.PeekMessagesAsync(
                It.Is<PeekMessagesRequestDto>(r => r.Queue == "unknown-queue"),
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(new NotFoundException("Queue", "unknown-queue"));

        var response = await SqsAdminTestClient.GetMessagesAsync(_httpClient, "unknown-queue", maxMessages: null, TestContext.Current.CancellationToken);

        response.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    // ReplayDlq

    [Fact]
    public async Task GivenValidRequest_WhenReplayRequested_ShouldReturnOkWithResult()
    {
        testFixture.Factory.SqsAdminServiceMock
            .Setup(x => x.ReplayDlqAsync(
                It.Is<ReplayDlqRequestDto>(r => r.Queue == "CadsBridgeFifo" && r.BatchSize == 5),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(new ReplayResultDto(4, 1, ["MessageId 'm1': boom"]));

        var response = await SqsAdminTestClient.ReplayDlqAsync(_httpClient, "CadsBridgeFifo", batchSize: 5, TestContext.Current.CancellationToken);

        response.StatusCode.Should().Be(HttpStatusCode.OK);

        var dto = await SqsAdminTestClient.ReadReplayDtoAsync(response, TestContext.Current.CancellationToken);
        dto.Should().NotBeNull();
        dto!.Queue.Should().Be("CadsBridgeFifo");
        dto.Moved.Should().Be(4);
        dto.Failed.Should().Be(1);
        dto.Errors.Should().ContainSingle();
    }

    [Theory]
    [InlineData(0)]
    [InlineData(11)]
    public async Task GivenInvalidBatchSize_WhenReplayRequested_ShouldReturnBadRequest(int batchSize)
    {
        var response = await SqsAdminTestClient.ReplayDlqAsync(_httpClient, "CadsBridgeFifo", batchSize, TestContext.Current.CancellationToken);

        response.StatusCode.Should().Be(HttpStatusCode.BadRequest);

        var problemDetails = await SqsAdminTestClient.ReadProblemDetailsAsync(response, TestContext.Current.CancellationToken);
        problemDetails.Should().NotBeNull();
        problemDetails!.Extensions.Should().ContainKey("errors");
    }

    [Fact]
    public async Task GivenNoBatchSize_WhenReplayRequested_ShouldSucceed()
    {
        testFixture.Factory.SqsAdminServiceMock
            .Setup(x => x.ReplayDlqAsync(
                It.Is<ReplayDlqRequestDto>(r => r.Queue == "CadsBridgeFifo" && r.BatchSize == 0),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(new ReplayResultDto(0, 0, []));

        var response = await SqsAdminTestClient.ReplayDlqAsync(_httpClient, "CadsBridgeFifo", batchSize: null, TestContext.Current.CancellationToken);

        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Fact]
    public async Task GivenQueueWithoutDlq_WhenReplayRequested_ShouldReturnUnprocessable()
    {
        testFixture.Factory.SqsAdminServiceMock
            .Setup(x => x.ReplayDlqAsync(
                It.Is<ReplayDlqRequestDto>(r => r.Queue == "CadsBridgeFifo"),
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(new UnprocessableException("Queue 'CadsCds' does not have a configured DLQ."));

        var response = await SqsAdminTestClient.ReplayDlqAsync(_httpClient, "CadsBridgeFifo", batchSize: 5, TestContext.Current.CancellationToken);

        response.StatusCode.Should().Be(HttpStatusCode.UnprocessableEntity);
    }

    [Fact]
    public async Task GivenNoToken_WhenReplayRequested_ShouldReturnUnauthorized()
    {
        var client = testFixture.Factory.CreateClient();

        var response = await SqsAdminTestClient.ReplayDlqAsync(client, "CadsBridgeFifo", batchSize: 5, TestContext.Current.CancellationToken);

        response.StatusCode.Should().Be(HttpStatusCode.Unauthorized);
    }

    [Fact]
    public async Task GivenValidRequest_WhenReplayRequested_ShouldLogAndInvokeServiceOnce()
    {
        testFixture.Factory.SqsAdminServiceMock
            .Setup(x => x.ReplayDlqAsync(
                It.Is<ReplayDlqRequestDto>(r => r.Queue == "CadsCds" && r.BatchSize == 7),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(new ReplayResultDto(7, 0, []));

        var response = await SqsAdminTestClient.ReplayDlqAsync(_httpClient, "CadsCds", batchSize: 7, TestContext.Current.CancellationToken);

        response.StatusCode.Should().Be(HttpStatusCode.OK);

        testFixture.Factory.SqsAdminServiceMock.Verify(x => x.ReplayDlqAsync(
            It.Is<ReplayDlqRequestDto>(r => r.Queue == "CadsCds" && r.BatchSize == 7),
            It.IsAny<CancellationToken>()), Times.Once);
    }
}