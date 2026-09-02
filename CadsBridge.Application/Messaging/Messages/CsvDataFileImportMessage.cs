namespace CadsBridge.Application.Messaging.Messages;

public class CsvDataFileImportMessage : MessageType
{
    public string Bucket { get; init; } = string.Empty; // e.g. abcdef-dev-dev1-livestockfeeds
    public string ObjectKey { get; init; } = string.Empty; // e.g. cads/cts/bulk/import-file.csv
    public string DestinationPrefix { get; init; } = string.Empty; // e.g. import/cts/bulk
    public string OracleEnvironment { get; init; } = string.Empty;
    public string Etag { get; init; } = string.Empty;
    public DateTime DiscoveredAtUtc { get; init; }
    public string CorrelationId { get; init; } = string.Empty;
    public string Identifier => $"{Bucket}/{ObjectKey}";
}