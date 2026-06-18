namespace CadsBridge.Endpoints.Requests;

public record CsvDataFileSplitRequest(
    List<CsvDataFileSplitRequestItem> Files
);