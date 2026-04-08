namespace CadsBridge.Application.Models;

public record ImportRequest(
    List<FileImportRequest> Files
);