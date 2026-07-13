namespace CadsBridge.Infrastructure.DataLoad.Csv.Files;

public record CtsmFilename(
    string App,
    string Env,
    string Type,
    string BatchId,
    string? PartNo,
    string TableName,
    string Timestamp
);