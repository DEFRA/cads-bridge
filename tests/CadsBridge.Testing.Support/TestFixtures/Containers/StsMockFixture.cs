using CadsBridge.Testing.Support.Fakes.Authentication;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using System.Text.Json;
using Xunit;

namespace CadsBridge.Testing.Support.TestFixtures.Containers;

// ReSharper disable once ClassNeverInstantiated.Global
public class StsMockFixture(string networkName) : IAsyncLifetime
{
    public const string NetworkAlias = "cads-sts-mock";

    private const int ContainerPort = 8080;
    private const string IssuerId = "sts";
    private const string StsClaimType = "https://sts.amazonaws.com/";
    private const string ScenarioParam = "scenario";
    private const string ExpiredScenario = "expired";

    // A fixed lifetime in the past (nbf before exp), so only the expiry check fails
    private const long ExpiredIssuedAt = 1_000_000_000;
    private const long ExpiredExpiresAt = ExpiredIssuedAt + 600;

    public IContainer StsContainer { get; private set; } = null!;
    public int StsPort => StsContainer.GetMappedPublicPort(ContainerPort);

    public string Issuer => $"http://{NetworkAlias}:{ContainerPort}/{IssuerId}";
    public string WellKnown => $"http://localhost:{StsPort}/{IssuerId}/.well-known/openid-configuration";
    public string TokenEndpoint => $"http://localhost:{StsPort}/{IssuerId}/token";

    public async ValueTask InitializeAsync()
    {
        DockerNetworkHelper.EnsureNetworkExists(networkName);

        var expiredClaims = TemplatedClaims();
        expiredClaims["iat"] = ExpiredIssuedAt;
        expiredClaims["nbf"] = ExpiredIssuedAt;
        expiredClaims["exp"] = ExpiredExpiresAt;

        // Mappings are matched in order; ${name} is replaced with the token request's form field of that name
        var config = ToJson(new
        {
            interactiveLogin = false,
            tokenCallbacks = new[]
            {
                new
                {
                    issuerId = IssuerId,
                    requestMappings = new[]
                    {
                        new { requestParam = ScenarioParam, match = ExpiredScenario, claims = expiredClaims },
                        new { requestParam = "grant_type", match = "client_credentials", claims = TemplatedClaims() }
                    }
                }
            }
        });

        StsContainer = new ContainerBuilder("ghcr.io/navikt/mock-oauth2-server:6.1.2")
            .WithName($"cads-sts-mock-{Guid.NewGuid()}")
            .WithPortBinding(ContainerPort, true)
            .WithEnvironment("JSON_CONFIG", config)
            .WithNetwork(networkName)
            .WithNetworkAliases(NetworkAlias)
            .WithWaitStrategy(Wait.ForUnixContainer()
                .UntilHttpRequestIsSucceeded(
                    req => req.ForPort(ContainerPort).ForPath($"/{IssuerId}/.well-known/openid-configuration"),
                    o => o.WithTimeout(TimeSpan.FromSeconds(25))))
            .Build();

        await StsContainer.StartAsync();
    }

    public async Task<string> CreateTokenAsync(StsTokenRequest request)
    {
        using var http = new HttpClient();

        var form = new Dictionary<string, string>
        {
            ["grant_type"] = "client_credentials",
            ["client_id"] = request.ServiceName,
            ["client_secret"] = "sts-mock",
            ["sub"] = request.Sub,
            ["aud"] = request.Aud,
            ["org_id"] = request.OrgId,
            ["aws_account"] = request.AwsAccount,
            ["service_name"] = request.ServiceName,
            ["environment"] = request.Environment
        };

        if (request.Expired)
            form[ScenarioParam] = ExpiredScenario;

        using var message = new HttpRequestMessage(HttpMethod.Post, TokenEndpoint)
        {
            Content = new FormUrlEncodedContent(form)
        };

        // The mock builds iss from the Host header, so it must name the container as the API sees it
        message.Headers.Host = $"{NetworkAlias}:{ContainerPort}";

        var response = await http.SendAsync(message);
        var json = await response.Content.ReadAsStringAsync();

        if (!response.IsSuccessStatusCode)
        {
            throw new HttpRequestException(
                $"STS mock token request failed for service '{request.ServiceName}': " +
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

        await Safe(() => StsContainer.DisposeAsync());

        GC.SuppressFinalize(this);

        if (error is not null)
            throw error;
    }

    private static string ToJson(object value) => JsonSerializer.Serialize(value);

    private static Dictionary<string, object> TemplatedClaims() => new()
    {
        ["sub"] = "${sub}",
        ["aud"] = "${aud}",
        [StsClaimType] = new Dictionary<string, object>
        {
            ["org_id"] = "${org_id}",
            ["aws_account"] = "${aws_account}",
            ["principal_tags"] = new Dictionary<string, string>
            {
                ["ServiceName"] = "${service_name}",
                ["Environment"] = "${environment}"
            }
        }
    };
}
