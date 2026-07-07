namespace CadsBridge.Infrastructure.Messaging.Factories;

public readonly struct FifoMessageMetadata(
    string messageGroupId,
    string messageDeduplicationId,
    string correlationId,
    IReadOnlyDictionary<string, string>? additionalAttributes = null)
{
    public string MessageGroupId { get; } = messageGroupId;
    public string MessageDeduplicationId { get; } = messageDeduplicationId;
    public string CorrelationId { get; } = correlationId;
    public IReadOnlyDictionary<string, string> AdditionalAttributes { get; } = additionalAttributes ?? new Dictionary<string, string>();
}