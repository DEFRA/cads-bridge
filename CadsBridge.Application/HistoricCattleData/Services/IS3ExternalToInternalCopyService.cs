using CadsBridge.Application.Models;

namespace CadsBridge.Application.HistoricCattleData.Services;

public interface IS3ExternalToInternalCopyService
{
    Task<bool> ExecAsync(ImportHistoricCattleFileJob job, CancellationToken cancellationToken = default);
}