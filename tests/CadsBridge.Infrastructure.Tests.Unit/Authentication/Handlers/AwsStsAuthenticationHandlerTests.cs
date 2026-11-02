using Amazon.Runtime;
using Amazon.SecurityToken;
using CadsBridge.Core.Exceptions;
using CadsBridge.Infrastructure.Authentication.DTOs;
using CadsBridge.Infrastructure.Authentication.Handlers;
using CadsBridge.Infrastructure.Authentication.Providers;
using CadsBridge.Testing.Support.Utilities.Http;
using FluentAssertions;
using Moq;
using System.Net;

namespace CadsBridge.Infrastructure.Tests.Unit.Authentication.Handlers;

public class AwsStsAuthenticationHandlerTests
{
    private const string Audience = "cads-data-service";

    private readonly Mock<IAwsStsProvider> _provider = new();
    private readonly StubHttpMessageHandler _inner = new(HttpStatusCode.OK);

    private HttpMessageInvoker CreateInvoker() =>
        new(new AwsStsAuthenticationHandler(_provider.Object, Audience) { InnerHandler = _inner });

    [Fact]
    public async Task SendAsync_ShouldAttachBearerToken_FromProvider()
    {
        _provider.Setup(x => x.GetCredentialsAsync(Audience, It.IsAny<CancellationToken>()))
            .ReturnsAsync(new IdentityToken("jwt-token", DateTimeOffset.UtcNow.AddMinutes(5)));
        using var invoker = CreateInvoker();

        var response = await invoker.SendAsync(new HttpRequestMessage(HttpMethod.Get, "http://test-api/resource"), TestContext.Current.CancellationToken);

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        var authorization = _inner.Requests.Should().ContainSingle().Which.Headers.Authorization;
        authorization!.Scheme.Should().Be("Bearer");
        authorization.Parameter.Should().Be("jwt-token");
    }

    [Fact]
    public async Task SendAsync_ShouldRequestTokenForConfiguredAudience_WithCallerCancellationToken()
    {
        using var cts = new CancellationTokenSource();
        _provider.Setup(x => x.GetCredentialsAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new IdentityToken("jwt-token", DateTimeOffset.UtcNow.AddMinutes(5)));
        using var invoker = CreateInvoker();

        await invoker.SendAsync(new HttpRequestMessage(HttpMethod.Get, "http://test-api/resource"), cts.Token);

        _provider.Verify(x => x.GetCredentialsAsync(Audience, cts.Token), Times.Once);
    }

    [Theory]
    [InlineData(HttpStatusCode.InternalServerError, "InternalFailure")]
    [InlineData(HttpStatusCode.ServiceUnavailable, "ServiceUnavailable")]
    [InlineData(HttpStatusCode.BadRequest, "ThrottlingException")]
    public async Task SendAsync_ShouldThrowRetryable_WhenStsFailureIsTransient(HttpStatusCode statusCode, string errorCode)
    {
        var stsException = StsException(statusCode, errorCode);
        SetupProviderThrows(stsException);
        using var invoker = CreateInvoker();

        var act = () => invoker.SendAsync(new HttpRequestMessage(HttpMethod.Get, "http://test-api/resource"), TestContext.Current.CancellationToken);

        (await act.Should().ThrowAsync<RetryableException>()).WithInnerException<AmazonSecurityTokenServiceException>();
        _inner.Requests.Should().BeEmpty();
    }

    [Theory]
    [InlineData("Throttling")]
    [InlineData("RequestLimitExceeded")]
    [InlineData("TooManyRequestsException")]
    public async Task SendAsync_ShouldThrowRetryable_WhenErrorCodeIsThrottling(string errorCode)
    {
        SetupProviderThrows(StsException(HttpStatusCode.BadRequest, errorCode));
        using var invoker = CreateInvoker();

        var act = () => invoker.SendAsync(new HttpRequestMessage(HttpMethod.Get, "http://test-api/resource"), TestContext.Current.CancellationToken);

        await act.Should().ThrowAsync<RetryableException>();
    }

    [Theory]
    [InlineData(HttpStatusCode.Forbidden, "AccessDeniedException")]
    [InlineData(HttpStatusCode.BadRequest, "ValidationError")]
    [InlineData(HttpStatusCode.Forbidden, "OutboundWebIdentityFederationDisabledException")]
    [InlineData(HttpStatusCode.Forbidden, "SessionDurationEscalationException")]
    [InlineData(HttpStatusCode.BadRequest, "JWTPayloadSizeExceededException")]
    public async Task SendAsync_ShouldThrowNonRetryable_WhenStsRejectsRequest(HttpStatusCode statusCode, string errorCode)
    {
        SetupProviderThrows(StsException(statusCode, errorCode));
        using var invoker = CreateInvoker();

        var act = () => invoker.SendAsync(new HttpRequestMessage(HttpMethod.Get, "http://test-api/resource"), TestContext.Current.CancellationToken);

        (await act.Should().ThrowAsync<NonRetryableException>()).WithInnerException<AmazonSecurityTokenServiceException>();
        _inner.Requests.Should().BeEmpty();
    }

    [Fact]
    public async Task SendAsync_ShouldThrowRetryable_WhenSdkClientFails()
    {
        SetupProviderThrows(new AmazonClientException("Unable to reach the credentials endpoint"));
        using var invoker = CreateInvoker();

        var act = () => invoker.SendAsync(new HttpRequestMessage(HttpMethod.Get, "http://test-api/resource"), TestContext.Current.CancellationToken);

        (await act.Should().ThrowAsync<RetryableException>()).WithInnerException<AmazonClientException>();
        _inner.Requests.Should().BeEmpty();
    }

    [Fact]
    public async Task SendAsync_ShouldNotWrapNonAwsExceptions()
    {
        SetupProviderThrows(new HttpRequestException("socket closed"));
        using var invoker = CreateInvoker();

        var act = () => invoker.SendAsync(new HttpRequestMessage(HttpMethod.Get, "http://test-api/resource"), TestContext.Current.CancellationToken);

        await act.Should().ThrowExactlyAsync<HttpRequestException>();
    }

    private void SetupProviderThrows(Exception exception) =>
        _provider.Setup(x => x.GetCredentialsAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(exception);

    private static AmazonSecurityTokenServiceException StsException(HttpStatusCode statusCode, string errorCode) =>
        new($"STS error {errorCode}") { StatusCode = statusCode, ErrorCode = errorCode };
}