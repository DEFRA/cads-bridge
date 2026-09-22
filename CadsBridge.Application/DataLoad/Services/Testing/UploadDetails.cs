namespace CadsBridge.Application.DataLoad.Services.Testing;

public record UploadDetails(string Key, long Size, string BucketName, DateTimeOffset UploadTime);