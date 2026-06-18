using Amazon.S3.Transfer;

namespace CadsBridge.Application.Storage.Transfer;

public interface ITransferUtilityAdapter
{
    Task UploadAsync(TransferUtilityUploadRequest request, CancellationToken token);
}