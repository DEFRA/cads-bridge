namespace CadsBridge.Endpoints.Requests;

public record CsvDataFileImportRequest(
    List<CsvDataFileImportRequestItem> Files
);