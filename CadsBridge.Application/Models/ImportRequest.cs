namespace CadsBridge.Application.Models;

public record ImportRequest(
    List<ImportRequestItem> Files
);