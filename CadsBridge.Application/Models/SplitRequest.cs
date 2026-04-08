namespace CadsBridge.Application.Models;

public record SplitRequest(
    List<FileSplitRequest> Files
);