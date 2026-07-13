using CadsBridge.Core.DataLoad.Jobs;

namespace CadsBridge.Application.DataLoad.Jobs;

// TODO: Remove Drops
public record CsvDataFileImportJob(
    string JobId,
    string SourceKey,
    string TargetKey, // Drop
    string Password, // Drop
    string Salt, // Drop
    SplitType SplitType, // Drop
    int? SplitValue, // Drop
    long? FileImportStatusId = null);
