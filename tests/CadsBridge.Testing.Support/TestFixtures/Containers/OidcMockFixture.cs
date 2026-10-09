using System.Text.Json;
using CadsBridge.Testing.Support.Constants;
using CadsBridge.Testing.Support.Fakes.Authentication;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using Xunit;

namespace CadsBridge.Testing.Support.TestFixtures.Containers;

// ReSharper disable once ClassNeverInstantiated.Global
public class OidcMockFixture(string networkName) : IAsyncLifetime
{
    private static readonly string[] s_baseUserClaims = ["email", "preferred_username", "name", "role"];

    private static readonly string[] s_passwordGrantTypes = ["password"];

    private static readonly string[] s_baseOidcScopes = ["openid", "profile", "email"];

    private static readonly string[] s_adminScopes =
    [
        TestAuthConstants.AzureAdAdminQueueManagerScope
    ];

    private static readonly string[] s_testUserScopes =
    [
        .. s_adminScopes,
        TestAuthConstants.AzureAdNoOpScope
    ];

    private static readonly (string Name, string DisplayName)[] s_apiScopeDefinitions =
    [
        (TestAuthConstants.AzureAdAdminQueueManagerScope, "Manage queues"),
        (TestAuthConstants.AzureAdNoOpScope, "No-op scope (audience only)")
    ];

    public IContainer OidcContainer { get; private set; } = null!;
    public int OidcPort => OidcContainer.GetMappedPublicPort(80);

    public string Issuer => $"http://localhost:{OidcPort}";
    public string WellKnown => $"{Issuer}/.well-known/openid-configuration";
    public string TokenEndpoint => $"{Issuer}/connect/token";

    public async ValueTask InitializeAsync()
    {
        DockerNetworkHelper.EnsureNetworkExists(networkName);

        var clients = ToJson(new object[]
        {
            new
            {
                ClientId = TestAuthConstants.AzureAdTestUserClientId,
                ClientSecrets = new[] { TestAuthConstants.AzureAdTestUserClient },
                AllowedGrantTypes = s_passwordGrantTypes,
                AllowedScopes = s_baseOidcScopes.Concat(s_testUserScopes),
                AccessTokenType = "Jwt",
                AlwaysSendClientClaims = true,
                AlwaysIncludeUserClaimsInIdToken = true
            }
        });

        var scopes = ToJson(s_apiScopeDefinitions.Select(s => new { s.Name, s.DisplayName }));

        var apiResources = ToJson(new[]
        {
            new
            {
                Name = TestAuthConstants.AzureAdCadsCdsAudience,
                Scopes = s_apiScopeDefinitions.Select(s => s.Name),
                UserClaims = s_baseUserClaims
            }
        });

        var users = ToJson(new[]
        {
            TestUser(
                subjectId: "9b6c9b3a-9e1e-4b6a-9d1e-6f1c2a7b5d3e",
                username: TestAuthConstants.AzureAdCadsAdminUsername,
                password: TestAuthConstants.AzureAdPassword,
                name: "Test Cads Admin",
                email: TestAuthConstants.AzureAdCadsAdminEmail,
                role: "cads-admin-superuser")
        });

        OidcContainer = new ContainerBuilder("ghcr.io/soluto/oidc-server-mock:0.6.0")
            .WithName($"cads-oidc-mock-{Guid.NewGuid()}")
            .WithPortBinding(80, true)
            .WithEnvironment("ASPNETCORE_ENVIRONMENT", "Development")
            .WithEnvironment("CLIENTS_CONFIGURATION_INLINE", clients)
            .WithEnvironment("SCOPES_CONFIGURATION_INLINE", scopes)
            .WithEnvironment("API_SCOPES_INLINE", scopes)
            .WithEnvironment("API_RESOURCES_INLINE", apiResources)
            .WithEnvironment("USERS_CONFIGURATION_INLINE", users)
            .WithEnvironment("ISSUER", "http://cads-oidc-mock")
            .WithNetwork(networkName)
            .WithNetworkAliases("cads-oidc-mock")
            .WithWaitStrategy(Wait.ForUnixContainer()
                .UntilHttpRequestIsSucceeded(
                    req => req.ForPort(80).ForPath("/.well-known/openid-configuration"),
                    o => o.WithTimeout(TimeSpan.FromSeconds(25))))
            .Build();

        await OidcContainer.StartAsync();
    }

    public async Task<string> CreateTokenAsync(TestTokenRequest request)
    {
        using var http = new HttpClient();

        var form = new Dictionary<string, string>
        {
            ["client_id"] = request.ClientId,
            ["client_secret"] = request.ClientSecret,
            ["scope"] = string.Join(" ", request.Scopes),
            ["resource"] = TestAuthConstants.AzureAdCadsCdsAudience
        };

        if (request.Username is null)
        {
            // client_credentials
            form["grant_type"] = "client_credentials";
        }
        else
        {
            // password (user token)
            form["grant_type"] = "password";
            form["username"] = request.Username;
            form["password"] = request.Password!;
        }

        var response = await http.PostAsync(TokenEndpoint, new FormUrlEncodedContent(form));
        var json = await response.Content.ReadAsStringAsync();

        if (!response.IsSuccessStatusCode)
        {
            throw new HttpRequestException(
                $"Token request failed for client '{request.ClientId}', user '{request.Username}': " +
                $"{(int)response.StatusCode} {json}");
        }

        return JsonDocument.Parse(json).RootElement.GetProperty("access_token").GetString()!;
    }

    public async ValueTask DisposeAsync()
    {
        Exception? error = null;

        async ValueTask Safe(Func<ValueTask> f)
        {
            try { await f(); }
            catch (Exception ex) { error ??= ex; }
        }

        await Safe(() => OidcContainer.DisposeAsync());

        GC.SuppressFinalize(this);

        if (error is not null)
            throw error;
    }

    private static string ToJson(object value) => JsonSerializer.Serialize(value);

    private static object AuthCodeClient(string id, string secret, IEnumerable<string> scopes) => new
    {
        ClientId = id,
        ClientSecrets = new[] { secret },
        AllowedGrantTypes = new[] { "authorization_code" },
        AllowedScopes = s_baseOidcScopes.Append("offline_access").Concat(scopes),
        RequirePkce = false,
        RedirectUris = new[] { "http://localhost:3000" },
        AllowOfflineAccess = true,
        AccessTokenType = "Jwt",
        AlwaysSendClientClaims = true,
        AlwaysIncludeUserClaimsInIdToken = true
    };

    private static object TestUser(
        string subjectId, string username, string password,
        string name, string email, string role) => new
        {
            SubjectId = subjectId,
            Username = username,
            Password = password,
            Claims = new[]
        {
            new { Type = "name", Value = name },
            new { Type = "email", Value = email },
            new { Type = "preferred_username", Value = username },
            new { Type = "role", Value = role }
        }
        };
}