namespace CadsBridge.Endpoints.Testing.Models;

public record CreateTestFileResponse(
    string FileName,
    string ExternalS3Bucket,
    string ExternalS3Key,
    long SizeBytes,
    DateTimeOffset UploadedAt);