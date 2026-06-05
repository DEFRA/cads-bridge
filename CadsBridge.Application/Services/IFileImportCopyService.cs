using CadsBridge.Application.Models;

namespace CadsBridge.Application.Services;

public interface IFileImportCopyService
{
    Task<bool> CopyWithRetryAsync(FileImportJob request, CancellationToken cancellationToken = default);
}