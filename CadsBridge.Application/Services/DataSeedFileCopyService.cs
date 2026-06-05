using Amazon.S3.Model;
using CadsBridge.Application.Models;
using CadsBridge.Core.Storage.Abstractions;
using CadsBridge.Core.Storage.Clients;
using CadsBridge.Core.Storage.FileSystem;
using Microsoft.Extensions.Logging;

namespace CadsBridge.Application.Services;

public class DataSeedFileCopyService(
    IS3ClientFactory s3ClientFactory,
    IFileSytemWrapper fileSystemWrapper,
    ILogger<DataSeedFileCopyService> logger) : IDataSeedFileCopyService
{
    public async Task<bool> ExecuteAsync(DataSeedImportJob request, CancellationToken cancellationToken)
    {
        try
        {
            var internalS3Info = s3ClientFactory.GetClientInfo<InternalStorageClient>();
            var internalS3 = internalS3Info.Client;
            await using var file = fileSystemWrapper.OpenRead(request.FileName);

            var putFile = new PutObjectRequest
            {
                BucketName = internalS3Info.BucketName,
                Key = request.TargetKey,
                InputStream = file,
                ContentType = "text/plain"
            };

            await internalS3.PutObjectAsync(putFile, cancellationToken);

            return true;
        }
        catch (Exception ex)
        {
            logger.LogError(
                ex,
                "Failed to copy data seed file from {fileName} to {targetKey}",
                request.FileName,
                request.TargetKey);

            return false;
        }
    }
}