using CadsBridge.Testing.Support.Constants;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Configurations;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Images;
using Xunit;

namespace CadsBridge.Testing.Support.TestFixtures.Containers;

public class ApiContainerFixture(IDictionary<string, string>? extraEnvironment = null) : IAsyncLifetime
{
    public IContainer? ApiContainer { get; private set; } = null;
    public HttpClient? HttpClient { get; private set; } = null;
    public LocalStackFixture LocalStackFixture { get; } = new();

    public async ValueTask InitializeAsync()
    {
        await LocalStackFixture.InitializeAsync();
        DockerNetworkHelper.EnsureNetworkExists(TestContainerConstants.NetworkName);

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
            .WithEnvironment("IMB_S3_ACCESS_KEY", "test")
            .WithEnvironment("IMB_S3_SECRET_KEY", "test")
            .WithEnvironment("LOCALSTACK_ENDPOINT", LocalStackFixture.NetworkServiceUrl)
            .WithEnvironment("AWS_REGION", LocalStackFixture.AwsRegion)
            .WithEnvironment("AWS_DEFAULT_REGION", LocalStackFixture.AwsRegion)
            .WithEnvironment("AWS_ACCESS_KEY_ID", LocalStackFixture.AwsAccessKeyId)
            .WithEnvironment("AWS_SECRET_ACCESS_KEY", LocalStackFixture.AwsSecretAccessKey)
            .WithEnvironment("DOTNET_SYSTEM_NET_SOCKETS_HTTP_USEIPV6", "false")
            .WithNetwork(TestContainerConstants.NetworkName)
            .WithNetworkAliases("cads_bridge")
            .WithWaitStrategy(Wait.ForUnixContainer()
                .UntilHttpRequestIsSucceeded(
                    req => req.ForPort(5550).ForPath("/health"),
                    o => o.WithTimeout(TimeSpan.FromSeconds(60))));

        if (extraEnvironment is not null)
            foreach (var (key, value) in extraEnvironment)
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