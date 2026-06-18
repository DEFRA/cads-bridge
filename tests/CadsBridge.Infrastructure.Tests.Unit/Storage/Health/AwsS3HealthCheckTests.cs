using Amazon.S3;
using Amazon.S3.Model;
using CadsBridge.Infrastructure.Storage.Abstractions;
using CadsBridge.Infrastructure.Storage.Health;
using FluentAssertions;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;
using Moq;
using System.Net;

namespace CadsBridge.Infrastructure.Tests.Unit.Storage.Health;

public class AwsS3HealthCheckTests
{
    private readonly AwsS3HealthCheck _sut;
    private readonly Mock<ILogger<AwsS3HealthCheck>> _logger = new();
    private readonly Mock<IAmazonS3> _s3Mock;

    public AwsS3HealthCheckTests()
    {
        Mock<IS3ClientFactory> factoryMock = new();
        _s3Mock = new();
        _sut = new AwsS3HealthCheck(factoryMock.Object, _logger.Object);
        factoryMock.Setup(x => x.GetRegisteredClientNames()).Returns(["client1"]);
        factoryMock.Setup(x => x.GetClient("client1")).Returns(_s3Mock.Object);
        factoryMock.Setup(x => x.GetClientBucketName("client1")).Returns("bucket1");
    }

    [Fact]
    public async Task ShouldReturnHealthy_WhenS3IsHealthy()
    {
        _s3Mock.Setup(x => x.ListObjectsV2Async(
                It.Is<ListObjectsV2Request>(x => x.BucketName == "bucket1"),
                It.IsAny<CancellationToken>())).
            ReturnsAsync(new ListObjectsV2Response() { HttpStatusCode = HttpStatusCode.OK });

        var result = await _sut.CheckHealthAsync(new HealthCheckContext(), TestContext.Current.CancellationToken);

        result.Status.Should().Be(HealthStatus.Healthy);
        result.Data["client1"].Should().BeEquivalentTo(new
        {
            Bucket = "bucket1",
            Status = "Healthy"
        });
    }

    [Fact]
    public async Task ShouldReturnUnhealthy_WhenS3IsUnhealthy()
    {
        _s3Mock.Setup(x => x.ListObjectsV2Async(
                It.Is<ListObjectsV2Request>(x => x.BucketName == "bucket1"),
                It.IsAny<CancellationToken>())).
            ReturnsAsync(new ListObjectsV2Response() { HttpStatusCode = HttpStatusCode.InternalServerError });

        var result = await _sut.CheckHealthAsync(new HealthCheckContext(), TestContext.Current.CancellationToken);

        result.Status.Should().Be(HealthStatus.Unhealthy);
        result.Data["client1"].Should().BeEquivalentTo(new
        {
            Bucket = "bucket1",
            Status = $"Degraded (Status: {HttpStatusCode.InternalServerError})"
        });
    }

    [Fact]
    public async Task ShouldReturnUnhealthy_WhenS3ThrowsAmazonS3Exception()
    {
        _s3Mock.Setup(x => x.ListObjectsV2Async(
                It.Is<ListObjectsV2Request>(x => x.BucketName == "bucket1"),
                It.IsAny<CancellationToken>())).
            Throws(new AmazonS3Exception { StatusCode = HttpStatusCode.NotFound });

        var result = await _sut.CheckHealthAsync(new HealthCheckContext(), TestContext.Current.CancellationToken);

        result.Status.Should().Be(HealthStatus.Unhealthy);
    }

    [Fact]
    public async Task ShouldReturnUnhealthy_WhenS3Throws()
    {
        _s3Mock.Setup(x => x.ListObjectsV2Async(
                It.Is<ListObjectsV2Request>(x => x.BucketName == "bucket1"),
                It.IsAny<CancellationToken>())).
            Throws<Exception>();

        var result = await _sut.CheckHealthAsync(new HealthCheckContext(), TestContext.Current.CancellationToken);

        result.Status.Should().Be(HealthStatus.Unhealthy);
    }
}