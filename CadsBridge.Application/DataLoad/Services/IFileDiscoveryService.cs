namespace CadsBridge.Application.DataLoad.Services;

public interface IFileDiscoveryService
{
    Task<List<string>> GetFileNames(string? prefix = null, CancellationToken cancellationToken = default);

    Task<bool> IsFileValid(string fileName, CancellationToken cancellationToken);

    Task EnQueueFileImportMessages(IReadOnlyList<string> objectKeys, CancellationToken cancellationToken);
}