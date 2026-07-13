namespace CadsBridge.Application.Messaging.Messages;

/*
{
  "bucket": "abcdef-dev-dev1-livestockfeeds",
  "objectKey": "path/to/import-file.dat",
  "oracleEnvironment": "PreProd",
  "etag": "s3-etag",
  "discoveredAtUtc": "2026-06-26T12:00:00Z",
  "correlationId": "generated-correlation-id",
  "identifier": "abcdef-dev-dev1-livestockfeeds/path/to/import-file.dat"
}
*/

public class CsvDataFileImportMessage : MessageType
{
    public string Bucket { get; init; } = string.Empty; // e.g. abcdef-dev-dev1-livestockfeeds
    public string ObjectKey { get; init; } = string.Empty; // e.g. cads/cts/bulk/import-file.csv
    public string OracleEnvironment { get; init; } = string.Empty; // e.g. Prod, PreProd
    public string Etag { get; init; } = string.Empty;
    public DateTime DiscoveredAtUtc { get; init; }
    public string CorrelationId { get; init; } = string.Empty;
    public string Identifier => $"{Bucket}/{ObjectKey}";
}
