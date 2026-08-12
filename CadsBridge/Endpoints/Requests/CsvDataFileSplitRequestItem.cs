using CadsBridge.Core.DataLoad;

namespace CadsBridge.Endpoints.Requests;

public record CsvDataFileSplitRequestItem(
    string Key,
    SplitType SplitType,
    int? SplitValue
);