using Amazon.SQS;
using Amazon.SQS.Model;
using CadsBridge.Infrastructure.Messaging.Configuration;
using CadsBridge.Infrastructure.Messaging.Health;
using CadsBridge.Testing.Support.Constants;
using FluentAssertions;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;
using Moq;
using System.Net;

namespace CadsBridge.Infrastructure.Tests.Unit.Messaging.Health;

public class AwsSqsHealthCheckTests
{
    private readonly Mock<IAmazonSQS> _sqsMock = new();
    private readonly Mock<IOptionsMonitor<QueueConsumerOptions>> _optionsMonitorMock = new();
    private readonly HealthCheckContext _healthCheckContext = new();

    private readonly QueueConsumerOptions _queueConsumerOptions = new()
    {
        Name = "TestQueue",
        QueueUrl = TestSqsConstants.TestQueueUrl,
        WaitTimeSeconds = 5,
        MaxNumberOfMessages = 10
    };

    private readonly AwsSqsHealthCheck<QueueConsumerOptions> _sut;

    public AwsSqsHealthCheckTests()
    {
        _optionsMonitorMock
            .Setup(x => x.Get(It.IsAny<string>()))
            .Returns(_queueConsumerOptions);

        _sqsMock
            .Setup(x => x.GetQueueAttributesAsync(It.IsAny<string>(), It.IsAny<List<string>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new GetQueueAttributesResponse { HttpStatusCode = HttpStatusCode.OK });

        _sut = new AwsSqsHealthCheck<QueueConsumerOptions>(_optionsMonitorMock.Object, _sqsMock.Object);

        _healthCheckContext = new HealthCheckContext
        {
            Registration = new HealthCheckRegistration(
                name: "TestQueue",
                instance: _sut,
                failureStatus: null,
                tags: null)
        };
    }

    [Fact]
    public async Task GivenValidQueueName_WhenCallingCheckHealthAsync_ShouldSucceed()
    {
        var result = await _sut.CheckHealthAsync(_healthCheckContext, CancellationToken.None);

        result.Status.Should().Be(HealthStatus.Healthy);
    }

    [Fact]
    public async Task GivenQueueAttributesAreMissing_WhenCallingCheckHealthAsync_ShouldReturnDegraded()
    {
        _sqsMock
            .Setup(x => x.GetQueueAttributesAsync(It.IsAny<string>(), It.IsAny<List<string>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(() => null);

        var result = await _sut.CheckHealthAsync(_healthCheckContext, CancellationToken.None);

        result.Status.Should().Be(HealthStatus.Degraded);
    }

    [Fact]
    public async Task GivenGetQueueAttributesFails_WhenCallingCheckHealthAsync_ShouldReturnUnhealthy()
    {
        _sqsMock
            .Setup(x => x.GetQueueAttributesAsync(It.IsAny<string>(), It.IsAny<List<string>>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new Exception("Service call failed."));

        var result = await _sut.CheckHealthAsync(_healthCheckContext, CancellationToken.None);

        result.Status.Should().Be(HealthStatus.Unhealthy);
    }

    [Fact]
    public async Task GivenGetQueueAttributesTimesOut_WhenCallingCheckHealthAsync_ShouldReturnUnhealthy()
    {
        _sqsMock
            .Setup(x => x.GetQueueAttributesAsync(It.IsAny<string>(), It.IsAny<List<string>>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new TaskCanceledException("Task has been cancelled"));

        var result = await _sut.CheckHealthAsync(_healthCheckContext, CancellationToken.None);

        result.Status.Should().Be(HealthStatus.Unhealthy);
        result.Exception.Should().NotBeNull().And.BeOfType<TimeoutException>();
        result.Exception.Message.Should().Be($"The queue check was cancelled, probably because it timed out after {_queueConsumerOptions.WaitTimeSeconds} seconds");
    }
}