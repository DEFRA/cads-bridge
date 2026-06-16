using CadsBridge.Application.Models;

namespace CadsBridge.Application.FileSplit.Services;

public interface IS3FileSplitterService
{
    Task<bool> ExecuteAsync(FileSplitJob job, CancellationToken cancellationToken);
}