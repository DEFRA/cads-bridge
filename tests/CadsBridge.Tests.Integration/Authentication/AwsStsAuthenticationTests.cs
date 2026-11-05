using System.Net;
using CadsBridge.Testing.Support.Constants;
using CadsBridge.Testing.Support.Fakes.Authentication;
using CadsBridge.Testing.Support.TestFixtures.Containers;
using FluentAssertions;

namespace CadsBridge.Tests.Integration.Authentication;

[Collection("CadsBridgeIntegration"), Trait("Dependence", "testcontainers")]
public class AwsStsAuthenticationTests(ApiContainerFixture apiContainerFixture)
{
    private const string ProtectedEndpoint = "health/details";

    // Must be listed in Acl:StsClients in appsettings.Test.json.
    private const string AllowedServiceName = "cads-data-service";

    [Fact]
    public async Task GivenValidStsToken_WhenCallingProtectedEndpoint_ShouldSucceed()
    {
        var response = await SendAsync(ValidRequest());

        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Fact]
    public async Task GivenStsTokenForAnotherAudience_WhenCallingProtectedEndpoint_ShouldBeUnauthorized()
    {
        var response = await SendAsync(ValidRequest() with { Aud = "https://another-service.test" });

        response.StatusCode.Should().Be(HttpStatusCode.Unauthorized);
    }

    [Fact]
    public async Task GivenStsTokenFromUntrustedOrg_WhenCallingProtectedEndpoint_ShouldBeUnauthorized()
    {
        var response = await SendAsync(ValidRequest() with { OrgId = "o-someoneelse" });

        response.StatusCode.Should().Be(HttpStatusCode.Unauthorized);
    }

    [Fact]
    public async Task GivenStsTokenForAnotherEnvironment_WhenCallingProtectedEndpoint_ShouldBeUnauthorized()
    {
        var response = await SendAsync(ValidRequest() with { Environment = "prod" });

        response.StatusCode.Should().Be(HttpStatusCode.Unauthorized);
    }

    [Fact]
    public async Task GivenStsTokenForUnknownService_WhenCallingProtectedEndpoint_ShouldBeUnauthorized()
    {
        var response = await SendAsync(ValidRequest() with { ServiceName = "not-on-the-allow-list" });

        response.StatusCode.Should().Be(HttpStatusCode.Unauthorized);
    }

    [Fact]
    public async Task GivenExpiredStsToken_WhenCallingProtectedEndpoint_ShouldBeUnauthorized()
    {
        var response = await SendAsync(ValidRequest() with { Expired = true });

        response.StatusCode.Should().Be(HttpStatusCode.Unauthorized);
    }

    private static StsTokenRequest ValidRequest() => new()
    {
        Sub = $"arn:aws:iam::123456789012:role/{AllowedServiceName}",
        Aud = TestAuthConstants.StsAudience,
        OrgId = TestAuthConstants.StsTrustedOrgId,
        AwsAccount = "123456789012",
        ServiceName = AllowedServiceName,
        Environment = TestAuthConstants.StsEnvironment
    };

    private async Task<HttpResponseMessage> SendAsync(StsTokenRequest request)
    {
        var client = await apiContainerFixture.CreateStsClientAsync(request);
        return await client.GetAsync(ProtectedEndpoint, TestContext.Current.CancellationToken);
    }
}