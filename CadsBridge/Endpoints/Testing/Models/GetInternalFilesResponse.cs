namespace CadsBridge.Endpoints.Testing.Models;

public record GetInternalFilesResponse(
    string FileName,
    IReadOnlyList<InternalFileChunkResponse> Chunks);