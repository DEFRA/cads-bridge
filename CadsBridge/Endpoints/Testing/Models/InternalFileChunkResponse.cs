namespace CadsBridge.Endpoints.Testing.Models;

public record InternalFileChunkResponse(
    string Key,
    int ChunkNo,
    long SizeBytes,
    long RecordCount);