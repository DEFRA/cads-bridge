namespace CadsBridge.Application.DataLoad.Services;

public interface IS3FileMetaDataService
{
    Task<long> GetRecordCountAsync(string s3Key, CancellationToken cancellationToken = default);
}