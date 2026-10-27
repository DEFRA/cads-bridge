using CadsBridge.Testing.Support.ApiClients;
using CadsBridge.Testing.Support.Constants;
using CadsBridge.Testing.Support.Fakes.Authentication;
using CadsBridge.Testing.Support.TestFixtures.Containers;
using FluentAssertions;
using Microsoft.IdentityModel.Tokens;
using System.Net;
using System.Text.Json;

namespace CadsBridge.Tests.Integration.Endpoints;

// Uses the SQS admin GetQueues endpoint because its policy requires a scope, so the happy path
// proves the caller's ACL scopes were granted rather than just that it authenticated.
[Collection("CadsBridgeIntegration"), Trait("Dependence", "testcontainers")]
public class StsAuthenticationTests(ApiContainerFixture apiContainerFixture)
{
    private static readonly StsTokenRequest s_validToken = new()
    {
        Sub = TestAuthConstants.StsRoleArn,
        Aud = TestAuthConstants.StsAudience,
        OrgId = TestAuthConstants.StsTrustedOrgId,
        AwsAccount = TestAuthConstants.StsAwsAccount,
        ServiceName = TestAuthConstants.StsServiceName,
        Environment = TestAuthConstants.StsEnvironment
    };

    [Fact]
    public async Task GivenStsMock_WhenTokenMinted_ShouldBeShapedLikeAnStsOutboundFederationToken()
    {
        var token = await apiContainerFixture.StsMockFixture.CreateTokenAsync(s_validToken);

        using var payload = JsonDocument.Parse(Base64UrlEncoder.Decode(token.Split('.')[1]));
        var root = payload.RootElement;

        root.GetProperty("iss").GetString().Should().Be(apiContainerFixture.StsMockFixture.Issuer);
        root.GetProperty("sub").GetString().Should().Be(TestAuthConstants.StsRoleArn);
        root.GetProperty("aud").GetString().Should().Be(TestAuthConstants.StsAudience);

        // A single JSON object value, not a stringified one or flattened claims
        var sts = root.GetProperty("https://sts.amazonaws.com/");
        sts.ValueKind.Should().Be(JsonValueKind.Object);
        sts.GetProperty("org_id").GetString().Should().Be(TestAuthConstants.StsTrustedOrgId);
        sts.GetProperty("aws_account").GetString().Should().Be(TestAuthConstants.StsAwsAccount);
        sts.GetProperty("principal_tags").GetProperty("ServiceName").GetString().Should().Be(TestAuthConstants.StsServiceName);
        sts.GetProperty("principal_tags").GetProperty("Environment").GetString().Should().Be(TestAuthConstants.StsEnvironment);
    }

    [Fact]
    public async Task GivenValidStsTokenWithScope_WhenGetQueuesRequested_ShouldReturnOk()
    {
        var response = await ExecuteTest(s_validToken);

        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Fact]
    public async Task GivenCallerOnAclWithoutScope_WhenGetQueuesRequested_ShouldReturnForbidden()
    {
        var response = await ExecuteTest(s_validToken with { ServiceName = TestAuthConstants.StsNoScopeServiceName });

        response.StatusCode.Should().Be(HttpStatusCode.Forbidden);
    }

    [Fact]
    public async Task GivenWrongAudience_WhenGetQueuesRequested_ShouldReturnUnauthorized()
    {
        var response = await ExecuteTest(s_validToken with { Aud = "https://some-other-service" });

        response.StatusCode.Should().Be(HttpStatusCode.Unauthorized);
    }

    [Fact]
    public async Task GivenWrongOrgId_WhenGetQueuesRequested_ShouldReturnUnauthorized()
    {
        var response = await ExecuteTest(s_validToken with { OrgId = "o-wrong" });

        response.StatusCode.Should().Be(HttpStatusCode.Unauthorized);
    }

    [Fact]
    public async Task GivenWrongEnvironment_WhenGetQueuesRequested_ShouldReturnUnauthorized()
    {
        var response = await ExecuteTest(s_validToken with { Environment = "Prod" });

        response.StatusCode.Should().Be(HttpStatusCode.Unauthorized);
    }

    [Fact]
    public async Task GivenCallerNotOnAcl_WhenGetQueuesRequested_ShouldReturnUnauthorized()
    {
        var response = await ExecuteTest(s_validToken with { ServiceName = "not-on-acl" });

        response.StatusCode.Should().Be(HttpStatusCode.Unauthorized);
    }

    [Fact]
    public async Task GivenExpiredToken_WhenGetQueuesRequested_ShouldReturnUnauthorized()
    {
        var response = await ExecuteTest(s_validToken with { Expired = true });

        response.StatusCode.Should().Be(HttpStatusCode.Unauthorized);
    }

    [Fact]
    public async Task GivenUntrustedIssuer_WhenGetQueuesRequested_ShouldReturnUnauthorized()
    {
        var response = await ExecuteTest(s_validToken with { UntrustedIssuer = true });

        response.StatusCode.Should().Be(HttpStatusCode.Unauthorized);
    }

    private async Task<HttpResponseMessage> ExecuteTest(StsTokenRequest request)
    {
        var client = await apiContainerFixture.CreateStsClientAsync(request);

        return await SqsAdminTestClient.GetQueuesAsync(client, TestContext.Current.CancellationToken);
    }
}