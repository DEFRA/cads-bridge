namespace CadsBridge.Testing.Support.Constants;

public static class TestEndpointConstants
{
    // SqsAdmin - route paths
    public const string SqsAdminQueuesRoot = "/api/v1/systemadmin/sqs/queues";

    // SqsAdmin - GetQueues
    public const string SqsAdminGetQueuesEndpoint = SqsAdminQueuesRoot;

    // SqsAdmin - GetMetrics
    public const string SqsAdminGetMetricsEndpoint = SqsAdminQueuesRoot + "/{0}/metrics";

    // SqsAdmin - GetMessages
    public const string SqsAdminGetMessagesEndpoint = SqsAdminQueuesRoot + "/{0}/messages";

    // SqsAdmin - GetDlqMessages
    public const string SqsAdminGetDlqMessagesEndpoint = SqsAdminQueuesRoot + "/{0}/dlq/messages";

    // SqsAdmin - GetDlqMetrics
    public const string SqsAdminGetDlqMetricsEndpoint = SqsAdminQueuesRoot + "/{0}/dlq/metrics";

    // SqsAdmin - ReplayDlq
    public const string SqsAdminReplayDlqEndpoint = SqsAdminQueuesRoot + "/{0}/dlq/replay";
}