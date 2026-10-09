using System.Net;
using System.Net.Http.Headers;
using CadsBridge.Testing.Support.Constants;
using CadsBridge.Testing.Support.Fakes.Authentication;
using CadsBridge.Testing.Support.TestFixtures.Containers.Configuration;
using CadsBridge.Testing.Support.Utilities.Authorization;
using CadsBridge.Testing.Support.Utilities.Http;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Images;
using Xunit;

namespace CadsBridge.Testing.Support.TestFixtures.Containers;

public abstract class ApiContainerFixtureBase : IAsyncLifetime
{
    private readonly string _networkName = $"integration-test-network-{Guid.NewGuid():N}";

    private readonly IDictionary<string, string>? _extraEnvironment;

    public IContainer? ApiContainer { get; private set; } = null;
    public HttpClient? HttpClient { get; private set; } = null;
    public LocalStackFixture LocalStackFixture { get; }
    public OidcMockFixture OidcMockFixture { get; }
    public TestAzureAdConfiguration? AzureAdConfig { get; set; }


    public ApiContainerFixtureBase(IDictionary<string, string>? extraEnvironment = null)
    {
        _extraEnvironment = extraEnvironment;
        LocalStackFixture = new LocalStackFixture(_networkName);
        OidcMockFixture = new OidcMockFixture(_networkName);
    }

    public async ValueTask InitializeAsync()
    {
        await LocalStackFixture.InitializeAsync();
        await OidcMockFixture.InitializeAsync();

        AzureAdConfig = new TestAzureAdConfiguration(OidcMockFixture);

        var builder = new ContainerBuilder("cads_bridge:latest")
            .WithImagePullPolicy(PullPolicy.Never)
            .WithEnvironment("ASPNETCORE_ENVIRONMENT", "Test")
            .WithEnvironment("ASPNETCORE_URLS", "http://0.0.0.0:5550")   // ✅ HTTP only - no HTTPS
            .WithPortBinding(5550, true)
            .WithEnvironment("AWS__ServiceURL", LocalStackFixture.NetworkServiceUrl)
            .WithEnvironment("Storage__Internal__BucketName", LocalStackFixture.InternalBucketName)
            .WithEnvironment("Storage__Internal__HealthcheckEnabled", "true")
            .WithEnvironment("Storage__External__BucketName", LocalStackFixture.ExternalBucketName)
            .WithEnvironment("Storage__External__HealthcheckEnabled", "true")
            .WithEnvironment("Storage__External__AccessKeySecretName", "IMB_S3_ACCESS_KEY")
            .WithEnvironment("Storage__External__SecretKeySecretName", "IMB_S3_SECRET_KEY")
            .WithEnvironment("Storage__External__EnvironmentName", "PreProd")
            .WithEnvironment("Messaging__Queues__CadsBridgeFifo__QueueUrl", LocalStackFixture.CadsBridgeFifoQueueUrl)
            .WithEnvironment("Messaging__Queues__CadsBridgeFifo__DlqQueueUrl", LocalStackFixture.CadsBridgeFifoDeadLetterQueueUrl)
            // The config section *key* here ("cads-bridge-admin-queue") is what SqsAdminService
            // reports back as each queue's "Name" (it returns the dictionary key, not a "Name"
            // property) - it must match the literal queue name the tests assert against.
            .WithEnvironment($"Messaging__Queues__{TestSqsConstants.CadsBridgeStandardQueueName}__QueueUrl", LocalStackFixture.CadsStandardQueueUrl)
            .WithEnvironment($"Messaging__Queues__{TestSqsConstants.CadsBridgeStandardQueueName}__DlqQueueUrl", LocalStackFixture.CadsStandardDeadLetterQueueUrl)
            // "Name" is required by QueuePublisherOptions, which binds against this same
            // "Messaging:Queues" section (shared with SqsAdminQueueOptions).
            // Without it, config binding for this entry throws at startup, breaking the
            // SystemAdmin FIFO queue publisher used elsewhere (e.g. FileImport processing).
            .WithEnvironment($"Messaging__Queues__{TestSqsConstants.CadsBridgeStandardQueueName}__Name", "CadsBridgeStandardTestClient")
            .WithEnvironment("Messaging__Queues__CadsBridgeFifo__HealthcheckEnabled", "true")
            .WithEnvironment("ApiClients__CdsApi__BaseUrl", "http://localhost:5555/")
            .WithEnvironment("ApiClients__CdsApi__BasicApiKey", "")
            .WithEnvironment("ApiClients__CdsApi__XApiKey", "")
            .WithEnvironment("ApiClients__CdsApi__HealthcheckEnabled", "false")
            .WithEnvironment("ApiClients__CdsApi__UseFakeClient", "true")
            .WithEnvironment("IMB_S3_ACCESS_KEY", "test")
            .WithEnvironment("IMB_S3_SECRET_KEY", "test")
            .WithEnvironment("AuthenticationConfiguration__ApiKey__Enabled", "true")
            .WithEnvironment("AuthenticationConfiguration__AzureAD__Enabled", "true")
            .WithEnvironment("AuthenticationConfiguration__AzureAD__Authority", AzureAdConfig.ContainerAuthority)
            .WithEnvironment("AuthenticationConfiguration__AzureAD__Audience", AzureAdConfig.Audience)
            .WithEnvironment("AuthenticationConfiguration__AzureAD__MetadataAddress", AzureAdConfig.ContainerMetadataAddress)
            .WithEnvironment("AuthenticationConfiguration__AzureAD__RequireHttpsMetadata", AzureAdConfig.RequireHttpsMetadata.ToString())
            .WithEnvironment("AuthenticationConfiguration__AzureAD__ValidateIssuer", "false")
            .WithEnvironment("AuthenticationConfiguration__AzureAD__ScopeClaimType", "scope")
            .WithEnvironment("AuthenticationConfiguration__AzureAD__RoleClaimType", "role")
            .WithEnvironment("EnableAdminEndpoints", "true")
            .WithEnvironment("DataLoad__Salt", "test-salt")
            .WithEnvironment("DataLoad__SplitValue", "5")
            .WithEnvironment("LOCALSTACK_ENDPOINT", LocalStackFixture.NetworkServiceUrl)
            .WithEnvironment("AWS_REGION", LocalStackFixture.AwsRegion)
            .WithEnvironment("AWS_DEFAULT_REGION", LocalStackFixture.AwsRegion)
            .WithEnvironment("AWS_ACCESS_KEY_ID", LocalStackFixture.AwsAccessKeyId)
            .WithEnvironment("AWS_SECRET_ACCESS_KEY", LocalStackFixture.AwsSecretAccessKey)
            .WithEnvironment("DOTNET_SYSTEM_NET_SOCKETS_HTTP_USEIPV6", "false")
            .WithEnvironment("Acl__Clients__TestClient__Secret", TestAuthConstants.BasicSecret)
            .WithEnvironment("Acl__Clients__TestClient__Scopes__0", "access")
            .WithNetwork(_networkName)
            .WithNetworkAliases("cads_bridge")
            .WithWaitStrategy(Wait.ForUnixContainer()
                .UntilHttpRequestIsSucceeded(
                    req => req.ForPort(5550).ForPath("/health"),
                    o => o.WithTimeout(TimeSpan.FromSeconds(60))));

        if (_extraEnvironment is not null)
            foreach (var (key, value) in _extraEnvironment)
                builder = builder.WithEnvironment(key, value);

        ApiContainer = builder.Build();
        try
        {
            await ApiContainer.StartAsync();
        }
        catch (Exception e)
        {
            var (stdout, stderr) = await ApiContainer.GetLogsAsync();
            throw new InvalidOperationException(
                $"cads_bridge container failed to become healthy.{Environment.NewLine}--- stdout ---{Environment.NewLine}{stdout}{Environment.NewLine}--- stderr ---{Environment.NewLine}{stderr}",
                e);
        }

        HttpClient = new HttpClient
        {
            BaseAddress = new Uri($"http://localhost:{ApiContainer.GetMappedPublicPort(5550)}")
        };
        HttpClient.AddBasicTestApiKey();
    }

    public async Task<HttpClient> CreateAzureAdClientAsync(TestTokenRequest request)
    {
        var token = await OidcMockFixture.CreateTokenAsync(request);

        var client = new HttpClient
        {
            BaseAddress = HttpClient.BaseAddress
        };

        client.DefaultRequestHeaders.Authorization =
            new AuthenticationHeaderValue("Bearer", token);

        return client;
    }


    public async ValueTask DisposeAsync()
    {
        Exception? error = null;
        async ValueTask Safe(Func<ValueTask> f)
        {
            try { await f(); }
            catch (Exception ex) { error ??= ex; }
        }

        await Safe(() => LocalStackFixture.DisposeAsync());
        try { HttpClient?.Dispose(); } catch (Exception ex) { error ??= ex; }
        await Safe(() => ApiContainer?.DisposeAsync() ?? default);
        await Safe(() => DockerNetworkHelper.DeleteNetwork(_networkName));

        GC.SuppressFinalize(this);
        if (error is not null) throw error;
    }
}