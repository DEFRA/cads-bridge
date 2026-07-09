using Amazon.S3.Model;
using CadsBridge.Application.DataLoad.Jobs;
using CadsBridge.Infrastructure.DataLoad.Csv.Contracts;
using CadsBridge.Infrastructure.DataLoad.Csv.Extensions;
using CadsBridge.Infrastructure.Storage.Factories;
using Microsoft.Extensions.Logging;

namespace CadsBridge.Infrastructure.DataLoad.Csv.Strategies;

public class CsvDataFileSplitterStrategyNone : ICsvDataFileSplitterStrategy
{
    public async Task Process(CsvDataFileSplitJob job, S3ClientFactory.ClientInfo internalS3Info, ILogger logger,
        CancellationToken cancellationToken)
    {
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