using CadsBridge.Application.DataLoad.Services;

namespace CadsBridge.Testing.Support.TestDoubles.Storage;

public class FakeS3FileMetaDataService   : IS3FileMetaDataService
{
    public long RecordCount { get; set; } = 100L;

    public Task<long> GetRecordCountAsync(string s3Key, CancellationToken cancellationToken = default)
    {
        return Task.FromResult(RecordCount);
    }
}