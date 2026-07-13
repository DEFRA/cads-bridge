using CadsBridge.Core.DataLoad.Jobs;

namespace CadsBridge.Application.DataLoad.Jobs;

// TODO: Remove Drops
public record CsvDataFileSplitJob(
    string JobId,
    string Key,
    string TargetFolder, // Drop
    SplitType SplitType, // Drop
    int? SplitValue, // Drop
    long? FileImportStatusId = null);