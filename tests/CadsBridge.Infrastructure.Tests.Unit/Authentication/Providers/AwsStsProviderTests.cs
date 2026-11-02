using Amazon.SecurityToken;
using Amazon.SecurityToken.Model;
using CadsBridge.Infrastructure.Authentication.Providers;
using FluentAssertions;
using Moq;

namespace CadsBridge.Infrastructure.Tests.Unit.Authentication.Providers;

public class AwsStsProviderTests
{
    private const string Audience = "cads-data-service";

    private readonly Mock<IAmazonSecurityTokenService> _sts = new();
    private readonly MutableTimeProvider _timeProvider = new(DateTimeOffset.Parse("2026-07-14T10:00:00Z"));
    private readonly AwsStsProvider _sut;

    public AwsStsProviderTests()
    {
        _sut = new AwsStsProvider(_sts.Object, _timeProvider);
    }

    [Fact]
    public async Task GetCredentialsAsync_ShouldRequestTokenForAudience_WhenCacheEmpty()
    {
        var expiration = _timeProvider.GetUtcNow().AddMinutes(5);
        SetupStsReturns("token-1", expiration);

        var token = await _sut.GetCredentialsAsync(Audience, TestContext.Current.CancellationToken);

        token.Token.Should().Be("token-1");
        token.Expiration.Should().Be(expiration);
        _sts.Verify(x => x.GetWebIdentityTokenAsync(
            It.Is<GetWebIdentityTokenRequest>(r =>
                r.Audience.Count == 1 &&
                r.Audience[0] == Audience &&
                r.SigningAlgorithm == "RS256" &&
                r.DurationSeconds == 300),
            It.IsAny<CancellationToken>()), Times.Once);
    }

    [Theory]
    [InlineData(31, 1)]
    [InlineData(30, 2)]
    [InlineData(-1, 2)]
    public async Task GetCredentialsAsync_ShouldOnlyRefresh_WhenWithinRefreshMargin(int secondsBeforeExpiry, int expectedStsCalls)
    {
        var expiration = _timeProvider.GetUtcNow().AddMinutes(5);
        SetupStsReturns("token-1", expiration);
        await _sut.GetCredentialsAsync(Audience, TestContext.Current.CancellationToken);

        _timeProvider.UtcNow = expiration.AddSeconds(-secondsBeforeExpiry);
        await _sut.GetCredentialsAsync(Audience, TestContext.Current.CancellationToken);

        VerifyStsCalled(Times.Exactly(expectedStsCalls));
    }

    [Fact]
    public async Task GetCredentialsAsync_ShouldReturnNewToken_WhenRefreshed()
    {
        var firstExpiration = _timeProvider.GetUtcNow().AddMinutes(5);
        var secondExpiration = firstExpiration.AddMinutes(5);
        _sts.SetupSequence(x => x.GetWebIdentityTokenAsync(It.IsAny<GetWebIdentityTokenRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(Response("token-1", firstExpiration))
            .ReturnsAsync(Response("token-2", secondExpiration));

        await _sut.GetCredentialsAsync(Audience, TestContext.Current.CancellationToken);
        _timeProvider.UtcNow = firstExpiration;
        var refreshed = await _sut.GetCredentialsAsync(Audience, TestContext.Current.CancellationToken);

        refreshed.Token.Should().Be("token-2");
        refreshed.Expiration.Should().Be(secondExpiration);
    }

    [Fact]
    public async Task GetCredentialsAsync_ShouldCacheTokensPerAudience()
    {
        SetupStsReturns("token", _timeProvider.GetUtcNow().AddMinutes(5));

        await _sut.GetCredentialsAsync("audience-a", TestContext.Current.CancellationToken);
        await _sut.GetCredentialsAsync("audience-b", TestContext.Current.CancellationToken);
        await _sut.GetCredentialsAsync("audience-a", TestContext.Current.CancellationToken);
        await _sut.GetCredentialsAsync("audience-b", TestContext.Current.CancellationToken);

        VerifyStsCalled(Times.Exactly(2));
        _sts.Verify(x => x.GetWebIdentityTokenAsync(
            It.Is<GetWebIdentityTokenRequest>(r => r.Audience[0] == "audience-a"),
            It.IsAny<CancellationToken>()), Times.Once);
        _sts.Verify(x => x.GetWebIdentityTokenAsync(
            It.Is<GetWebIdentityTokenRequest>(r => r.Audience[0] == "audience-b"),
            It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task GetCredentialsAsync_ShouldCallStsOnce_WhenCalledConcurrently()
    {
        var pending = new TaskCompletionSource<GetWebIdentityTokenResponse>(TaskCreationOptions.RunContinuationsAsynchronously);
        _sts.Setup(x => x.GetWebIdentityTokenAsync(It.IsAny<GetWebIdentityTokenRequest>(), It.IsAny<CancellationToken>()))
            .Returns(pending.Task);

        var calls = Enumerable.Range(0, 10)
            .Select(_ => _sut.GetCredentialsAsync(Audience, TestContext.Current.CancellationToken))
            .ToList();

        pending.SetResult(Response("token-1", _timeProvider.GetUtcNow().AddMinutes(5)));
        var tokens = await Task.WhenAll(calls);

        tokens.Should().AllSatisfy(t => t.Token.Should().Be("token-1"));
        VerifyStsCalled(Times.Once());
    }

    [Fact]
    public async Task GetCredentialsAsync_ShouldNotCache_WhenStsOmitsExpiration()
    {
        SetupStsReturns("token-1", expiration: null);

        await _sut.GetCredentialsAsync(Audience, TestContext.Current.CancellationToken);
        await _sut.GetCredentialsAsync(Audience, TestContext.Current.CancellationToken);

        VerifyStsCalled(Times.Exactly(2));
    }

    [Fact]
    public async Task GetCredentialsAsync_ShouldPropagateExceptionAndRetryNextCall_WhenStsFails()
    {
        _sts.SetupSequence(x => x.GetWebIdentityTokenAsync(It.IsAny<GetWebIdentityTokenRequest>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new AmazonSecurityTokenServiceException("throttled"))
            .ReturnsAsync(Response("token-1", _timeProvider.GetUtcNow().AddMinutes(5)));

        var act = () => _sut.GetCredentialsAsync(Audience, TestContext.Current.CancellationToken);

        await act.Should().ThrowAsync<AmazonSecurityTokenServiceException>();
        var token = await _sut.GetCredentialsAsync(Audience, TestContext.Current.CancellationToken);
        token.Token.Should().Be("token-1");
        VerifyStsCalled(Times.Exactly(2));
    }

    private void SetupStsReturns(string token, DateTimeOffset? expiration) =>
        _sts.Setup(x => x.GetWebIdentityTokenAsync(It.IsAny<GetWebIdentityTokenRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(Response(token, expiration));

    private static GetWebIdentityTokenResponse Response(string token, DateTimeOffset? expiration) =>
        new() { WebIdentityToken = token, Expiration = expiration?.UtcDateTime };

    private void VerifyStsCalled(Times times) =>
        _sts.Verify(x => x.GetWebIdentityTokenAsync(It.IsAny<GetWebIdentityTokenRequest>(), It.IsAny<CancellationToken>()), times);

    private sealed class MutableTimeProvider(DateTimeOffset now) : TimeProvider
    {
        public DateTimeOffset UtcNow { get; set; } = now;

        public override DateTimeOffset GetUtcNow() => UtcNow;
    }
}