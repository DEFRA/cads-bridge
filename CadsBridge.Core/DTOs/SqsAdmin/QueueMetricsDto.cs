namespace CadsBridge.Core.DTOs.SqsAdmin;

public record QueueMetricsDto(
    long ApproximateNumberOfMessages,
    long ApproximateNumberOfMessagesNotVisible,
    long ApproximateNumberOfMessagesDelayed,
    long OldestMessageAgeSeconds);