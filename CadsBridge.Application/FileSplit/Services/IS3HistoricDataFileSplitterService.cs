using CadsBridge.Application.Models;

namespace CadsBridge.Application.FileSplit.Services;

public interface IS3HistoricDataFileSplitterService
{
    Task<bool> ExecuteAsync(HistoricDataFileSplitJob job, CancellationToken cancellationToken);
}