using Amazon.S3;
using Amazon.S3.Model;
using Amazon.SQS;
using Amazon.SQS.Model;
using CadsBridge.Testing.Support.Constants;
using Testcontainers.LocalStack;
using Xunit;

namespace CadsBridge.Testing.Support.TestFixtures.Containers;

public class LocalStackFixture : IAsyncLifetime
{
    public static LocalStackContainer? LocalStackContainer { get; private set; }

    public IAmazonS3 S3Client { get; private set; } = null!;
    public IAmazonSQS SqsClient { get; private set; } = null!;

    public string? SqsEndpoint { get; private set; }
    public static string ServiceUrl => $"http://localhost:{LocalStackContainer!.GetMappedPublicPort(TestContainerConstants.LocalStackPort)}";
    public static string NetworkServiceUrl => $"http://{TestContainerConstants.NetworkAlias}:{TestContainerConstants.LocalStackPort}";
    public static string CadsBridgeFifoQueueUrl => $"http://sqs.eu-west-2.localhost.localstack.cloud:{TestContainerConstants.LocalStackPort}/000000000000/{TestSqsConstants.CadsBridgeFifoQueueName}";
    public static string CadsBridgeFifoDeadLetterQueueUrl => $"http://sqs.eu-west-2.localhost.localstack.cloud:{TestContainerConstants.LocalStackPort}/000000000000/{TestSqsConstants.CadsBridgeFifoDeadLetterQueueName}";

    public const string AwsAccessKeyId = "test";
    public const string AwsSecretAccessKey = "test";
    public const string InternalBucketName = "cads-bridge-internal-bucket";
    public const string ExternalBucketName = "cads-bridge-external-bucket";
    public const string AwsRegion = TestAwsConstants.AwsRegion;
    private static Amazon.Runtime.BasicAWSCredentials GetBasicAWSCredentials => new(AwsAccessKeyId, AwsSecretAccessKey);

    public async ValueTask InitializeAsync()
    {
        DockerNetworkHelper.EnsureNetworkExists(TestContainerConstants.NetworkName);

        LocalStackContainer = new LocalStackBuilder("localstack/localstack:3.0.2")
            .WithEnvironment("SERVICES", "s3")
            .WithEnvironment("DEBUG", "1")
            .WithEnvironment("AWS_DEFAULT_REGION", AwsRegion)
            .WithEnvironment("AWS_ACCESS_KEY_ID", AwsAccessKeyId)
            .WithEnvironment("AWS_SECRET_ACCESS_KEY", AwsSecretAccessKey)
            .WithNetwork(TestContainerConstants.NetworkName)
            .WithNetworkAliases(TestContainerConstants.NetworkAlias)
            .Build();

        await LocalStackContainer.StartAsync();

        InitialiseClients();
        await InitialiseResourcesAsync();
        await VerifyResourcesAsync();
    }

    private void InitialiseClients()
    {
        S3Client = new AmazonS3Client(AwsAccessKeyId, AwsSecretAccessKey, new AmazonS3Config
        {
            ServiceURL = ServiceUrl,
            ForcePathStyle = true
        });

        SqsClient = new AmazonSQSClient(GetBasicAWSCredentials, new AmazonSQSConfig
        {
            ServiceURL = ServiceUrl,
            AuthenticationRegion = AwsRegion,
            UseHttp = true
        });

        SqsEndpoint = SqsClient.Config.ServiceURL!;
    }

    private async Task InitialiseResourcesAsync()
    {
        await S3Client.PutBucketAsync(new PutBucketRequest { BucketName = InternalBucketName });
        await S3Client.PutBucketAsync(new PutBucketRequest { BucketName = ExternalBucketName });

        var cadsBridgeFifoDlqCreated = await SqsClient.CreateQueueAsync(new CreateQueueRequest { QueueName = TestSqsConstants.CadsBridgeFifoDeadLetterQueueName });
        var cadsBridgeFifoDlqAttr = await SqsClient.GetQueueAttributesAsync(new GetQueueAttributesRequest
        {
            QueueUrl = cadsBridgeFifoDlqCreated.QueueUrl,
            AttributeNames = ["QueueArn"]
        });

        var cadsBridgeFifoQueueCreated = await SqsClient.CreateQueueAsync(new CreateQueueRequest { QueueName = TestSqsConstants.CadsBridgeFifoQueueName });

        if (CadsBridgeFifoDeadLetterQueueUrl != cadsBridgeFifoDlqCreated.QueueUrl || CadsBridgeFifoQueueUrl != cadsBridgeFifoQueueCreated.QueueUrl)
        {
            throw new ApplicationException("Localstack queues have unexpected urls");
        }

        var redrivePolicy = $"{{\"deadLetterTargetArn\":\"{cadsBridgeFifoDlqAttr.QueueARN}\",\"maxReceiveCount\":\"3\"}}";
        await SqsClient.SetQueueAttributesAsync(new SetQueueAttributesRequest
        {
            QueueUrl = CadsBridgeFifoQueueUrl,
            Attributes = new Dictionary<string, string>
            {
                { "RedrivePolicy", redrivePolicy }
            }
        });
    }

    private async Task VerifyResourcesAsync()
    {
        await S3Client.ListObjectsV2Async(new ListObjectsV2Request { BucketName = InternalBucketName });
        await S3Client.ListObjectsV2Async(new ListObjectsV2Request { BucketName = ExternalBucketName });

        await SqsClient.GetQueueAttributesAsync(TestSqsConstants.CadsBridgeFifoDeadLetterQueueName, ["All"], CancellationToken.None);
        await SqsClient.GetQueueAttributesAsync(TestSqsConstants.CadsBridgeFifoQueueName, ["All"], CancellationToken.None);
    }

    public async ValueTask DisposeAsync()
    {
        Exception? error = null;

        async ValueTask Safe(Func<ValueTask> f)
        {
            try { await f(); }
            catch (Exception ex) { error ??= ex; }
        }

        // Synchronous disposals wrapped safely
        try { S3Client?.Dispose(); }
        catch (Exception ex) { error ??= ex; }

        try { SqsClient?.Dispose(); }
        catch (Exception ex) { error ??= ex; }

        // Async disposal using the same Safe pattern
        await Safe(() => LocalStackContainer?.DisposeAsync() ?? default);

        GC.SuppressFinalize(this);

        if (error is not null)
            throw error;
    }
}