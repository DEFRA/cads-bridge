using Amazon.S3.Model;
using CadsBridge.Application.DataLoad.Csv.Abstractions;
using CadsBridge.Application.DataLoad.Jobs;
using CadsBridge.Core.DataLoad.Jobs;
using CadsBridge.Infrastructure.DataLoad.Csv.Extensions;
using CadsBridge.Infrastructure.Storage.Abstractions;
using CadsBridge.Infrastructure.Storage.Clients;
using Microsoft.Extensions.Logging;

namespace CadsBridge.Infrastructure.DataLoad.Csv.Strategies;

public class CsvDataFileSplitterStrategyNone(
    IS3ClientFactory s3ClientFactory,
    ILogger<CsvDataFileSplitterStrategyNone> logger) :
    ICsvDataFileSplitterStrategy
{
    public SplitType SplitType => SplitType.None;

    public async Task Process(CsvDataFileSplitJob job, CancellationToken cancellationToken)
    {
        if (logger.IsEnabled(LogLevel.Information))
            logger.LogInformation("Copying {Key} without splitting", job.Key);

        var internalS3Info = s3ClientFactory.GetClientInfo<InternalStorageClient>();
        var s3 = internalS3Info.Client;
        using var response = await s3.GetObjectAsync(
            new GetObjectRequest { BucketName = internalS3Info.BucketName, Key = job.Key },
            cancellationToken);

        await using var memoryStream = new MemoryStream();
        await response.ResponseStream.CopyToAsync(memoryStream, cancellationToken);

        await s3.UploadChunkAsync(
            internalS3Info.BucketName,
            job.Key.FormatKey(job.TargetFolder),
            memoryStream,
            cancellationToken: cancellationToken);
    }
}