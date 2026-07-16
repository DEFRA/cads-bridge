namespace CadsBridge.Application.Messaging.Models;

public class UnwrappedMessage
{
    public string MessageId { get; init; } = default!;
    public string CorrelationId { get; init; } = default!;
    public string Subject { get; init; } = "Default";
    public string Payload { get; init; } = default!;
    public Dictionary<string, string>? Attributes { get; init; }

    public string MessageGroupId => Attributes?.TryGetValue("MessageGroupId", out var g) == true ? g : string.Empty;
    public string MessageDeduplicationId => Attributes?.TryGetValue("MessageDeduplicationId", out var d) == true ? d : string.Empty;
}