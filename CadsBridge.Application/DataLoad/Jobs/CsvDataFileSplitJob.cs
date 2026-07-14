namespace CadsBridge.Application.DataLoad.Jobs;

public record CsvDataFileSplitJob(
    string JobId,
    string SourceKey,
    long? FileImportId = null
);