using CadsBridge.Application.Models;

namespace CadsBridge.Application.Services;

public interface IS3FileSplitterService
{
    Task<bool> ExecuteAsync(FileSplitJob job, CancellationToken cancellationToken);
}