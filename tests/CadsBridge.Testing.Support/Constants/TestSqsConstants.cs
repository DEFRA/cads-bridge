namespace CadsBridge.Testing.Support.Constants;

public static class TestSqsConstants
{
    public static string TestQueueUrl => $"{TestAwsConstants.AwsServiceUrl.TrimEnd('/')}/000000000000/test-queue";
    public static string TestQueueDlqUrl => $"{TestAwsConstants.AwsServiceUrl.TrimEnd('/')}/000000000000/test-queue-deadletter";

    public const string CadsBridgeFifoQueueName = "cads-bridge-queue";
    public const string CadsBridgeFifoDeadLetterQueueName = "cads-bridge-queue-deadletter";
}