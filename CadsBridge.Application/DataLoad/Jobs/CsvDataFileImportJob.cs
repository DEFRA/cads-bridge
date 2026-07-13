namespace CadsBridge.Application.DataLoad.Jobs;

public record CsvDataFileImportJob(
    string JobId,
    string SourceKey,
    long? FileImportStatusId = null);