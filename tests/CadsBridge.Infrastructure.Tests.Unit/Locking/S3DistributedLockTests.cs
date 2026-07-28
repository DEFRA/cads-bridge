using Amazon.S3;
using Amazon.S3.Model;
using CadsBridge.Infrastructure.Locking;
using CadsBridge.Infrastructure.Storage.Abstractions;
using CadsBridge.Infrastructure.Storage.Clients;
using CadsBridge.Infrastructure.Storage.Factories;
using CadsBridge.Testing.Support.Utilities.Logging;
using FluentAssertions;
using Microsoft.Extensions.Logging;
using Moq;
using System.Net;
using System.Text;
using System.Text.Json;

namespace CadsBridge.Infrastructure.Tests.Unit.Locking;

public class S3DistributedLockTests
{
    private const string Bucket = "internal-bucket";
    private const string LockName = "delta-scan";
    private const string ExpectedKey = "locks/delta-scan.lock";

    private readonly Mock<IAmazonS3> _s3 = new();
    private readonly MutableTimeProvider _timeProvider = new(DateTimeOffset.Parse("2026-07-14T10:00:00Z"));
    private readonly Mock<IS3ClientFactory> _factory = new();
    private readonly S3DistributedLock _sut;

    public S3DistributedLockTests()
    {
        _factory.Setup(x => x.GetClientInfo<InternalStorageClient>())
                .Returns(new S3ClientFactory.ClientInfo(_s3.Object, Bucket));

        _sut = CreateSut();
    }

    private S3DistributedLock CreateSut(
        S3DistributedLockOptions? options = null,
        ILogger<S3DistributedLock>? logger = null) =>
        new(_factory.Object, options ?? new S3DistributedLockOptions(), _timeProvider,
            logger ?? Mock.Of<ILogger<S3DistributedLock>>());

    private static Mock<ILogger<S3DistributedLock>> EnabledLogger() =>
        new Mock<ILogger<S3DistributedLock>>().EnableAllLogLevels();

    [Fact]
    public async Task TryAcquireAsync_ShouldReturnTrue_WhenLockCreatedAtomically()
    {
        SetupConditionalCreate(succeeds: true);

        var acquired = await _sut.TryAcquireAsync(LockName, TestContext.Current.CancellationToken);

        acquired.Should().BeTrue();
        _s3.Verify(x => x.PutObjectAsync(
            It.Is<PutObjectRequest>(r =>
                r.BucketName == Bucket &&
                r.Key == ExpectedKey &&
                r.IfNoneMatch == "*"),
            It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task TryAcquireAsync_ShouldReturnFalse_WhenLockHeldAndNotExpired()
    {
        SetupConditionalCreate(succeeds: false);
        SetupExisting(expiresInMinutes: 5, etag: "\"held\"");

        var acquired = await _sut.TryAcquireAsync(LockName, TestContext.Current.CancellationToken);

        acquired.Should().BeFalse();
        VerifyNoTakeoverAttempted();
    }

    [Fact]
    public async Task TryAcquireAsync_ShouldTakeOver_WhenExistingLockIsStale()
    {
        SetupConditionalCreate(succeeds: false);
        SetupExisting(expiresInMinutes: -1, etag: "\"stale-etag\"");
        SetupConditionalTakeOver(ifMatch: "\"stale-etag\"", succeeds: true);

        var acquired = await _sut.TryAcquireAsync(LockName, TestContext.Current.CancellationToken);

        acquired.Should().BeTrue();
        _s3.Verify(x => x.PutObjectAsync(
            It.Is<PutObjectRequest>(r => r.IfMatch == "\"stale-etag\""),
            It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task TryAcquireAsync_ShouldReturnFalse_WhenTakeOverLosesRace()
    {
        SetupConditionalCreate(succeeds: false);
        SetupExisting(expiresInMinutes: -1, etag: "\"stale-etag\"");
        SetupConditionalTakeOver(ifMatch: "\"stale-etag\"", succeeds: false);

        var acquired = await _sut.TryAcquireAsync(LockName, TestContext.Current.CancellationToken);

        acquired.Should().BeFalse();
    }

    [Fact]
    public async Task TryAcquireAsync_ShouldRetryCreate_WhenLockReleasedBetweenCreateAndRead()
    {
        _s3.SetupSequence(x => x.PutObjectAsync(
                It.Is<PutObjectRequest>(r => r.IfNoneMatch == "*"),
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(PreconditionFailed())
            .ReturnsAsync(new PutObjectResponse { ETag = "\"new\"" });

        _s3.Setup(x => x.GetObjectAsync(
                It.IsAny<GetObjectRequest>(),
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(NotFound());

        var acquired = await _sut.TryAcquireAsync(LockName, TestContext.Current.CancellationToken);

        acquired.Should().BeTrue();
        _s3.Verify(x => x.PutObjectAsync(
            It.Is<PutObjectRequest>(r => r.IfNoneMatch == "*"),
            It.IsAny<CancellationToken>()), Times.Exactly(2));
    }

    [Fact]
    public async Task ReleaseAsync_ShouldConditionallyDelete_WhenLockIsHeld()
    {
        SetupConditionalCreate(succeeds: true, etag: "\"owned-etag\"");
        await _sut.TryAcquireAsync(LockName, TestContext.Current.CancellationToken);

        _s3.Setup(x => x.DeleteObjectAsync(
                It.IsAny<DeleteObjectRequest>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(new DeleteObjectResponse());

        await _sut.ReleaseAsync(LockName, TestContext.Current.CancellationToken);

        _s3.Verify(x => x.DeleteObjectAsync(
            It.Is<DeleteObjectRequest>(r =>
                r.BucketName == Bucket &&
                r.Key == ExpectedKey &&
                r.IfMatch == "\"owned-etag\""),
            It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task ReleaseAsync_ShouldDoNothing_WhenLockNotHeld()
    {
        await _sut.ReleaseAsync(LockName, TestContext.Current.CancellationToken);

        _s3.Verify(x => x.DeleteObjectAsync(
            It.IsAny<DeleteObjectRequest>(),
            It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task ReleaseAsync_ShouldSwallow_WhenLeaseAlreadyTakenOver()
    {
        SetupConditionalCreate(succeeds: true, etag: "\"owned-etag\"");
        await _sut.TryAcquireAsync(LockName, TestContext.Current.CancellationToken);

        _s3.Setup(x => x.DeleteObjectAsync(
                It.IsAny<DeleteObjectRequest>(),
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(PreconditionFailed());

        var release = async () => await _sut.ReleaseAsync(LockName, TestContext.Current.CancellationToken);

        await release.Should().NotThrowAsync();
    }

    [Fact]
    public async Task TryAcquireAsync_ShouldLogDebug_WhenLockAcquiredAndLoggingEnabled()
    {
        SetupConditionalCreate(succeeds: true);
        var sut = CreateSut(logger: EnabledLogger().Object);

        var acquired = await sut.TryAcquireAsync(LockName, TestContext.Current.CancellationToken);

        acquired.Should().BeTrue();
    }

    [Fact]
    public async Task TryAcquireAsync_ShouldLogDebug_WhenLockHeldAndNotExpiredAndLoggingEnabled()
    {
        SetupConditionalCreate(succeeds: false);
        SetupExisting(expiresInMinutes: 5, etag: "\"held\"");
        var sut = CreateSut(logger: EnabledLogger().Object);

        var acquired = await sut.TryAcquireAsync(LockName, TestContext.Current.CancellationToken);

        acquired.Should().BeFalse();
    }

    [Fact]
    public async Task TryAcquireAsync_ShouldReturnFalse_WhenAllAttemptsExhausted()
    {
        var options = new S3DistributedLockOptions { MaxAcquireAttempts = 1 };
        var sut = CreateSut(options: options, logger: EnabledLogger().Object);

        _s3.Setup(x => x.PutObjectAsync(
                It.Is<PutObjectRequest>(r => r.IfNoneMatch == "*"), It.IsAny<CancellationToken>()))
            .ThrowsAsync(PreconditionFailed());

        _s3.Setup(x => x.GetObjectAsync(It.IsAny<GetObjectRequest>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(NotFound()); // returns null from TryReadAsync → continue → but no attempts left

        var acquired = await sut.TryAcquireAsync(LockName, TestContext.Current.CancellationToken);

        acquired.Should().BeFalse();
    }

    [Fact]
    public async Task TryAcquireAsync_ShouldRetry_WhenReadDeserializationReturnsNull()
    {
        _s3.SetupSequence(x => x.PutObjectAsync(
                It.Is<PutObjectRequest>(r => r.IfNoneMatch == "*"), It.IsAny<CancellationToken>()))
            .ThrowsAsync(PreconditionFailed())
            .ReturnsAsync(new PutObjectResponse { ETag = "\"new\"" });

        _s3.Setup(x => x.GetObjectAsync(It.IsAny<GetObjectRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(() => new GetObjectResponse
            {
                ETag = "\"something\"",
                ResponseStream = new MemoryStream(Encoding.UTF8.GetBytes("null"))
            });

        var acquired = await _sut.TryAcquireAsync(LockName, TestContext.Current.CancellationToken);

        acquired.Should().BeTrue();
    }

    [Fact]
    public async Task TryAcquireAsync_ShouldLogInformation_WhenTakeoverSucceedsAndLoggingEnabled()
    {
        SetupConditionalCreate(succeeds: false);
        SetupExisting(expiresInMinutes: -1, etag: "\"stale-etag\"");
        SetupConditionalTakeOver(ifMatch: "\"stale-etag\"", succeeds: true);
        var sut = CreateSut(logger: EnabledLogger().Object);

        var acquired = await sut.TryAcquireAsync(LockName, TestContext.Current.CancellationToken);

        acquired.Should().BeTrue();
    }

    [Fact]
    public async Task ReleaseAsync_ShouldLogDebug_WhenLockReleasedAndLoggingEnabled()
    {
        SetupConditionalCreate(succeeds: true, etag: "\"owned-etag\"");
        var sut = CreateSut(logger: EnabledLogger().Object);
        await sut.TryAcquireAsync(LockName, TestContext.Current.CancellationToken);

        _s3.Setup(x => x.DeleteObjectAsync(It.IsAny<DeleteObjectRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new DeleteObjectResponse());

        var release = async () => await sut.ReleaseAsync(LockName, TestContext.Current.CancellationToken);

        await release.Should().NotThrowAsync();
    }

    [Fact]
    public async Task ReleaseAsync_ShouldLogDebug_WhenLeaseAlreadyTakenOverAndLoggingEnabled()
    {
        SetupConditionalCreate(succeeds: true, etag: "\"owned-etag\"");
        var sut = CreateSut(logger: EnabledLogger().Object);
        await sut.TryAcquireAsync(LockName, TestContext.Current.CancellationToken);

        _s3.Setup(x => x.DeleteObjectAsync(It.IsAny<DeleteObjectRequest>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(PreconditionFailed());

        var release = async () => await sut.ReleaseAsync(LockName, TestContext.Current.CancellationToken);

        await release.Should().NotThrowAsync();
    }

    private void SetupConditionalCreate(bool succeeds, string etag = "\"created\"")
    {
        var setup = _s3.Setup(x => x.PutObjectAsync(
            It.Is<PutObjectRequest>(r => r.IfNoneMatch == "*"),
            It.IsAny<CancellationToken>()));

        if (succeeds)
        {
            setup.ReturnsAsync(new PutObjectResponse { ETag = etag });
        }
        else
        {
            setup.ThrowsAsync(PreconditionFailed());
        }
    }

    private void SetupConditionalTakeOver(string ifMatch, bool succeeds)
    {
        var setup = _s3.Setup(x => x.PutObjectAsync(
            It.Is<PutObjectRequest>(r => r.IfMatch == ifMatch),
            It.IsAny<CancellationToken>()));

        if (succeeds)
        {
            setup.ReturnsAsync(new PutObjectResponse { ETag = "\"taken-over\"" });
        }
        else
        {
            setup.ThrowsAsync(PreconditionFailed());
        }
    }

    private void SetupExisting(int expiresInMinutes, string etag)
    {
        var now = _timeProvider.GetUtcNow();
        var entry = new LockEntry(
            Owner: Guid.NewGuid().ToString("N"),
            AcquiredAtUtc: now.AddMinutes(-5),
            ExpiresAtUtc: now.AddMinutes(expiresInMinutes));

        _s3.Setup(x => x.GetObjectAsync(
                It.Is<GetObjectRequest>(r => r.BucketName == Bucket && r.Key == ExpectedKey),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(() => new GetObjectResponse
            {
                ETag = etag,
                ResponseStream = new MemoryStream(Encoding.UTF8.GetBytes(JsonSerializer.Serialize(entry)))
            });
    }

    private void VerifyNoTakeoverAttempted() =>
        _s3.Verify(x => x.PutObjectAsync(
            It.Is<PutObjectRequest>(r => r.IfMatch != null),
            It.IsAny<CancellationToken>()), Times.Never);

    private static AmazonS3Exception PreconditionFailed() =>
        new("Precondition Failed") { StatusCode = HttpStatusCode.PreconditionFailed };

    private static AmazonS3Exception NotFound() =>
        new("Not Found") { StatusCode = HttpStatusCode.NotFound };

    private sealed class MutableTimeProvider(DateTimeOffset now) : TimeProvider
    {
        public override DateTimeOffset GetUtcNow() => now;
    }
}