namespace CadsBridge.Application.DataLoad.Jobs;

public record CsvDataFileImportJob(
    string SourceKey,
    string DestinationPrefix,
    string? CorrelationId = null);