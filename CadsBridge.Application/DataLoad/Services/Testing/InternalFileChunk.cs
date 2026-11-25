namespace CadsBridge.Application.DataLoad.Services.Testing;

public sealed record InternalFileChunk(string Key, int ChunkNo, long SizeBytes, long RecordCount);