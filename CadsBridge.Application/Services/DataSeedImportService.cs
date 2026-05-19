using System.Collections.Concurrent;
using System.Threading.Channels;
using Amazon.S3;
using Amazon.S3.Model;
using CadsBridge.Application.Models;
using CadsBridge.Core.Storage.Abstractions;
using CadsBridge.Core.Storage.Clients;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace CadsBridge.Application.Services;


public class DataSeedImportService(Channel<DataSeedImportJob> channel, ILogger<DataSeedImportService> logger, IS3ClientFactory s3ClientFactory) : BackgroundService
{
    private readonly int _maxParallelFileTransfers = 4;

    protected override async Task ExecuteAsync(CancellationToken cancellationToken = default)
    {
        var semaphore = new SemaphoreSlim(_maxParallelFileTransfers);
        var runningTasks = new ConcurrentBag<Task>();

        await foreach (var request in channel.Reader.ReadAllAsync(cancellationToken))
        {
            if (cancellationToken.IsCancellationRequested)
            {
                logger.LogInformation("Cancellation requested, aborting split");
                return;
            }

            await semaphore.WaitAsync(cancellationToken);

            var task = Task.Run(async () =>
            {
                try
                {
                    var result = await CopyFile(request, cancellationToken);
                    if (result)
                    {
                        logger.LogInformation("Successfully imported data seed file {fileName} to {targetKey}", request.FileName, request.TargetKey);
                    }
                    else
                    {
                        logger.LogError("Failed to import data seed file {fileName} to {targetKey}", request.FileName, request.TargetKey);
                    }
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "Failed to import data seed file {fileName} to {targetKey}", request.FileName, request.TargetKey);
                }
                finally
                {
                    semaphore.Release();
                }
            }, cancellationToken);
            runningTasks.Add(task);
        }
        await Task.WhenAll(runningTasks);
    }

    private async Task<bool> CopyFile(DataSeedImportJob request, CancellationToken cancellationToken)
    {
        try
        {
            var internalS3Info = s3ClientFactory.GetClientInfo<InternalStorageClient>();
            var internalS3 = internalS3Info.Client;
            await using var file = File.OpenRead(request.FileName);
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
            logger.LogError(ex, "Failed to copy data seed file from {fileName} to {targetKey}", request.FileName, request.TargetKey);
            return false;
        }
    }
}