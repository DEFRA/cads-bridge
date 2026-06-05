using CadsBridge.Application.Models;

namespace CadsBridge.Application.Services;

public interface IDataSeedFileCopyService
{
    Task<bool> ExecuteAsync(DataSeedImportJob request, CancellationToken cancellationToken);
}