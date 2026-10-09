namespace CadsBridge.Core.DTOs.SqsAdmin;

public record QueueMessageDto(
    string MessageId,
    string Body,
    IReadOnlyDictionary<string, string> Attributes,
    DateTimeOffset? SentTimestamp);