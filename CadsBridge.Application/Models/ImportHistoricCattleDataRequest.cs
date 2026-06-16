namespace CadsBridge.Application.Models;

public record ImportHistoricCattleDataRequest(
    List<ImportHistoricFileRequestItem> Files
);