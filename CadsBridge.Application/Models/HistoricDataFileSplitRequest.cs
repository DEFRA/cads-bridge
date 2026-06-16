namespace CadsBridge.Application.Models;

public record HistoricDataFileSplitRequest(
    List<HistoricDataFileSplitRequestItem> Files
);