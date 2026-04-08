namespace CadsBridge.Application.Models;

public record SplitRequest(
    List<SplitRequestItem> Files
);