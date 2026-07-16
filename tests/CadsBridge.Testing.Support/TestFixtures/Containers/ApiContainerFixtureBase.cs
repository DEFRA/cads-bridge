using CadsBridge.Testing.Support.Constants;
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

    public ApiContainerFixtureBase(IDictionary<string, string>? extraEnvironment = null)
    {
        _extraEnvironment = extraEnvironment;
        LocalStackFixture = new LocalStackFixture(_networkName);
    }

    public async ValueTask InitializeAsync()
    {
        await LocalStackFixture.InitializeAsync();

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
            .WithEnvironment("Messaging__Queues__CadsBridgeFifo__QueueUrl", TestSqsConstants.CadsBridgeFifoQueueName)
            .WithEnvironment("Messaging__Queues__CadsBridgeFifo__DlqQueueUrl", TestSqsConstants.CadsBridgeFifoDeadLetterQueueName)
            .WithEnvironment("Messaging__Queues__CadsBridgeFifo__HealthcheckEnabled", "true")
            .WithEnvironment("ApiClients__CdsApi__BaseUrl", "http://localhost:5555/")
            .WithEnvironment("ApiClients__CdsApi__BasicApiKey", "")
            .WithEnvironment("ApiClients__CdsApi__XApiKey", "")
            .WithEnvironment("ApiClients__CdsApi__HealthcheckEnabled", "false")
            .WithEnvironment("ApiClients__CdsApi__UseFakeClient", "true")
            .WithEnvironment("IMB_S3_ACCESS_KEY", "test")
            .WithEnvironment("IMB_S3_SECRET_KEY", "test")
            .WithEnvironment("DataLoad__Salt", "test-salt")
            .WithEnvironment("DataLoad__SplitValue", "5")
            .WithEnvironment("LOCALSTACK_ENDPOINT", LocalStackFixture.NetworkServiceUrl)
            .WithEnvironment("AWS_REGION", LocalStackFixture.AwsRegion)
            .WithEnvironment("AWS_DEFAULT_REGION", LocalStackFixture.AwsRegion)
            .WithEnvironment("AWS_ACCESS_KEY_ID", LocalStackFixture.AwsAccessKeyId)
            .WithEnvironment("AWS_SECRET_ACCESS_KEY", LocalStackFixture.AwsSecretAccessKey)
            .WithEnvironment("DOTNET_SYSTEM_NET_SOCKETS_HTTP_USEIPV6", "false")
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
        await ApiContainer.StartAsync();

        HttpClient = new HttpClient
        {
            BaseAddress = new Uri($"http://localhost:{ApiContainer.GetMappedPublicPort(5550)}")
        };
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

        GC.SuppressFinalize(this);
        if (error is not null) throw error;
    }
}