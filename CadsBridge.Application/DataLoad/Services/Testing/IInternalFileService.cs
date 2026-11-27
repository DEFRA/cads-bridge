namespace CadsBridge.Application.DataLoad.Services.Testing;

public interface IInternalFileService
{
    Task<IReadOnlyList<InternalFileChunk>> ListChunksAsync(string fileName, CancellationToken cancellationToken = default);
}