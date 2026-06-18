using CadsBridge.Core.DataLoad.Jobs;

namespace CadsBridge.Endpoints.Requests;

public record CsvDataFileImportRequestItem(
    string JobId,
    string sourceKey,
    string targetKey,
    string Password,
    string Salt,
    SplitType SplitType,
    int? SplitValue
);