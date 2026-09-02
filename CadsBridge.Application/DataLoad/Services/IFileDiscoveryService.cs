namespace CadsBridge.Application.DataLoad.Services;

public interface IFileDiscoveryService
{
    Task<List<string>> GetFileNames(string? prefix = null, CancellationToken cancellationToken = default);

    Task<bool> IsFileValid(string fileName, CancellationToken cancellationToken);

    /// <summary>
    /// Publishes an import message for each external object key. <paramref name="destinationPrefix"/> is the
    /// internal bucket prefix the file will be copied to (e.g. <c>import/cts/bulk</c>).
    /// </summary>
    Task EnQueueFileImportMessages(IReadOnlyList<string> objectKeys, string destinationPrefix, CancellationToken cancellationToken);
}
