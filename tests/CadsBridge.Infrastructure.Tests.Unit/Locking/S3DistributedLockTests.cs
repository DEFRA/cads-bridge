using System.Net;
using System.Text;
using System.Text.Json;
using Amazon.S3;
using Amazon.S3.Model;
using CadsBridge.Infrastructure.Locking;
using CadsBridge.Infrastructure.Storage.Abstractions;
using CadsBridge.Infrastructure.Storage.Clients;
using CadsBridge.Infrastructure.Storage.Factories;
using FluentAssertions;
using Microsoft.Extensions.Logging;
using Moq;

namespace CadsBridge.Infrastructure.Tests.Unit.Locking;

public class S3DistributedLockTests
{
    private const string Bucket = "internal-bucket";
    private const string LockName = "delta-scan";
    private const string ExpectedKey = "locks/delta-scan.lock";

    private readonly Mock<IAmazonS3> _s3 = new();
    private readonly MutableTimeProvider _timeProvider = new(DateTimeOffset.Parse("2026-07-14T10:00:00Z"));
    private readonly S3DistributedLock _sut;

    public S3DistributedLockTests()
    {
        var factory = new Mock<IS3ClientFactory>();
        factory.Setup(x => x.GetClientInfo<InternalStorageClient>())
               .Returns(new S3ClientFactory.ClientInfo(_s3.Object, Bucket));

        _sut = new S3DistributedLock(
            factory.Object,
            new S3DistributedLockOptions(),
            _timeProvider,
            Mock.Of<ILogger<S3DistributedLock>>());
    }

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
        // First create fails (exists), read returns 404 (released), second create succeeds.
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