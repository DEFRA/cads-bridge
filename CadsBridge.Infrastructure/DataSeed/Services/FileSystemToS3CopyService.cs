using Amazon.S3.Model;
using CadsBridge.Application.DataSeed.Services;
using CadsBridge.Application.Models;
using CadsBridge.Application.Services;
using CadsBridge.Core.Storage.Abstractions;
using CadsBridge.Core.Storage.Clients;
using CadsBridge.Core.Storage.FileSystem;
using Microsoft.Extensions.Logging;

namespace CadsBridge.Infrastructure.DataSeed.Services;

public class FileSystemToS3CopyService(
    IS3ClientFactory s3ClientFactory,
    IFileSytemWrapper fileSystemWrapper,
    ILogger<FileSystemToS3CopyService> logger) : IFileSystemToS3CopyService
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
        }
        catch (Exception ex)
        {
            logger.LogError(
                ex,
                "Failed to copy data seed file from {FileName} to {TargetKey}",
                request.FileName,
                request.TargetKey);

            return false;
        }

        return true;
    }
}