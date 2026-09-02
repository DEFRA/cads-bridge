namespace CadsBridge.Endpoints.Requests;

public record CsvDataFileImportRequestItem(
    string sourceKey,
    string? destinationPrefix = null
);