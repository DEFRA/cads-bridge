namespace CadsBridge.Application.Models;

public record StartImportRequest(
    List<FileImportRequest> Files
);