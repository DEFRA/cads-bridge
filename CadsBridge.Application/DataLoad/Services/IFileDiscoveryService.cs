namespace CadsBridge.Application.DataLoad.Services;

public interface IFileDiscoveryService
{
    Task<List<string>> GetFileNames(CancellationToken cancellationToken);
}