using CadsBridge.Core.DataLoad.Jobs;

namespace CadsBridge.Application.DataLoad.Jobs;

public record CsvDataFileImportJob(
    string JobId,
    string SourceKey,
    string TargetKey,
    string Password,
    string Salt,
    SplitType SplitType,
    int? SplitValue);