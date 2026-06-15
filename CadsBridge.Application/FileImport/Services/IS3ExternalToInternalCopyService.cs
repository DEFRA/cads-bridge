using CadsBridge.Application.Models;

namespace CadsBridge.Application.FileImport.Services;

public interface IS3ExternalToInternalCopyService
{
    Task<bool> ExecAsync(FileImportJob job, CancellationToken cancellationToken = default);
}