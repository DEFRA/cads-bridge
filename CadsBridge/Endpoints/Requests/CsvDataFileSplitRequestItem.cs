using CadsBridge.Core.DataLoad.Jobs;

namespace CadsBridge.Endpoints.Requests;

public record CsvDataFileSplitRequestItem(
    string Key,
    SplitType SplitType,
    int? SplitValue
);