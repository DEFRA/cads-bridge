namespace CadsBridge.Infrastructure.SqsAdmin.Configuration;

public class SqsAdminQueueOptions
{
    public static readonly string QueuesSectionName = "Messaging:Queues";

    public required string QueueUrl { get; set; }
    public string? DlqQueueUrl { get; set; }
    public int DefaultReplayBatchSize { get; set; } = 10;
}