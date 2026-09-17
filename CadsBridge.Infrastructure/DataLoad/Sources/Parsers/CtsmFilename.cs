namespace CadsBridge.Infrastructure.DataLoad.Sources.Parsers;

public record CtsmFilename(
    string App,
    string Env,
    string Type,
    string BatchId,
    string? PartNo,
    string TableName,
    string Timestamp
);