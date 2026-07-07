namespace CadsBridge.Infrastructure.Messaging.Configuration;

public record QueuePublisherOptions
{
    public required string Name { get; set; }
    public required string QueueUrl { get; set; }
}