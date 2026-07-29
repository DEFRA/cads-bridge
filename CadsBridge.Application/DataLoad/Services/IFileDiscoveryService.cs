namespace CadsBridge.Application.DataLoad.Services;

public interface IFileDiscoveryService
{
    Task<List<string>> GetFileNames(CancellationToken cancellationToken);

    Task<bool> IsFileValid(string fileName, CancellationToken cancellationToken);
}