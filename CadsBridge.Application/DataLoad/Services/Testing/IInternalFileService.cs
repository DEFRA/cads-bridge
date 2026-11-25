namespace CadsBridge.Application.DataLoad.Services.Testing;

public interface IInternalFileService
{
    Task<IReadOnlyList<InternalFileChunk>> ListChunksAsync(string fileName, CancellationToken cancellationToken = default);
}

public sealed record InternalFileChunk(string Key, int ChunkNo, long SizeBytes, long RecordCount);
