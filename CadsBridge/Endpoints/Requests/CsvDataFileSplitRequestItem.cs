using CadsBridge.Core.DataLoad.Jobs;

namespace CadsBridge.Endpoints.Requests;

public record CsvDataFileSplitRequestItem(
    string Key,
    string TargetFolder,
    SplitType SplitType,
    int? SplitValue
);