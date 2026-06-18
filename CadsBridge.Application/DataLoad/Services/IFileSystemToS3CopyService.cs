using CadsBridge.Application.DataLoad.Jobs;

namespace CadsBridge.Application.DataLoad.Services;

public interface IFileSystemToS3CopyService
{
    Task<bool> ExecuteAsync(DataSeedFileLoadJob request, CancellationToken cancellationToken);
}