using CadsBridge.Application.DataLoad.Scanning;

namespace CadsBridge.Application.DataLoad.Services.Testing;

public interface IS3UploadService
{
    Task<UploadDetails> UploadAsync(string key, string data, DataSourceType dataSource, bool overwriteExisting, CancellationToken cancellationToken = default);
}