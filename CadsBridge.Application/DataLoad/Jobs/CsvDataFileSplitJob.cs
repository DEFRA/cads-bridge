using CadsBridge.Core.DataLoad.Jobs;

namespace CadsBridge.Application.DataLoad.Jobs;

public record CsvDataFileSplitJob(
    string JobId,
    string Key,
    string TargetFolder,
    SplitType SplitType,
    int? SplitValue,
    long? FileImportStatusId = null
);