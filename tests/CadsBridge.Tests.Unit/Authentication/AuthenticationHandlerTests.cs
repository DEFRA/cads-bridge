using System.Net;
using CadsBridge.Testing.Support.Constants;
using CadsBridge.Testing.Support.Utilities.Authorization;
using CadsBridge.Tests.Unit.TestFixtures;
using FluentAssertions;
namespace CadsBridge.Tests.Unit.Authentication;

public class AuthenticationHandlerTests
{
    private static CadsBridgedWebApplicationFactory GetFactory(bool useFakeAuth = false)
    {
        var factory = new CadsBridgedWebApplicationFactory(useFakeAuth: useFakeAuth);
        return factory;
    }

    [Fact]
    public async Task GivenTheAadPolicy_WhenDbAdminEndpointRequested_AndNoTokenProvided_ReturnsUnauthorized()
    {
        var factory = GetFactory();
        var client = factory.CreateClient();

        var response = await client.GetAsync("test-auth/azuread/sqs-admin", TestContext.Current.CancellationToken);

        response.StatusCode.Should().Be(HttpStatusCode.Unauthorized);
    }

    [Fact]
    public async Task GivenTheAadDbAdminExecutePolicy_WhenDbAdminEndpointRequested_AndValidTokenWithRoleAndScopeProvided_ReturnsOk()
    {
        var factory = GetFactory(true);
        var client = factory.CreateClient();
        client.AddJwt();

        var response = await client.GetAsync("test-auth/azuread/sqs-admin", TestContext.Current.CancellationToken);

        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Fact]
    public async Task GivenTheAadPolicy_WhenDbAdminEndpointRequested_AndScopeClaimMissing_ReturnsForbidden()
    {
        var factory = GetFactory(true);
        var client = factory.CreateClient();
        client.AddJwt(TestAuthConstants.FakeJwtMissingSqsAdminScope);

        var response = await client.GetAsync("test-auth/azuread/sqs-admin", TestContext.Current.CancellationToken);

        response.StatusCode.Should().Be(HttpStatusCode.Forbidden);
    }
}