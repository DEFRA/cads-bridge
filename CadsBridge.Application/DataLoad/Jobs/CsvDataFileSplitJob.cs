namespace CadsBridge.Application.DataLoad.Jobs;

public record CsvDataFileSplitJob(
    string SourceKey,
    long? FileImportId = null,
    long TotalRowsToProcess = 0,
    string? CorrelationId = null
);