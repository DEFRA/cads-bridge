using CadsBridge.Application.Models;

namespace CadsBridge.Application.DataSeed.Services;

public interface IFileSystemToS3CopyService
{
    Task<bool> ExecuteAsync(DataSeedImportJob request, CancellationToken cancellationToken);
}