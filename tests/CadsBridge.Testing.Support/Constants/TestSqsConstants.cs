namespace CadsBridge.Testing.Support.Constants;

public static class TestSqsConstants
{
    public static string TestQueueUrl => $"{TestAwsConstants.AwsServiceUrl.TrimEnd('/')}/000000000000/test-queue";
    public static string TestQueueDlqUrl => $"{TestAwsConstants.AwsServiceUrl.TrimEnd('/')}/000000000000/test-queue-deadletter";

    public const string CadsBridgeFifoQueueName = "cads-bridge-queue.fifo";
    public const string CadsBridgeFifoDeadLetterQueueName = "cads-bridge-queue-deadletter.fifo";

    // Standard (non-FIFO) queue, dedicated to exercising generic SQS admin tooling
    // (peek/metrics/replay) in integration tests. FIFO queues combined with the
    // VisibilityTimeout=0 "peek" semantics used by SqsAdminService are not reliably
    // supported by LocalStack's SQS emulation, so admin tooling is tested against a
    // standard queue instead of the shared FIFO queue used by StorageBridge.
    public const string CadsBridgeStandardQueueName = "cads-bridge-admin-queue";
    public const string CadsBridgeStandardDeadLetterQueueName = "cads-bridge-admin-queue-deadletter";
}