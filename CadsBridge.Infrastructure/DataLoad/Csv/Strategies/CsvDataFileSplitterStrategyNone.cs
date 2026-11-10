using Amazon.S3.Model;
using CadsBridge.Application.DataLoad.Csv.Abstractions;
using CadsBridge.Application.DataLoad.Jobs;
using CadsBridge.Application.Encryption;
using CadsBridge.Core.DataLoad;
using CadsBridge.Infrastructure.DataLoad.Csv.Extensions;
using CadsBridge.Infrastructure.Encryption.Configuration;
using CadsBridge.Infrastructure.Storage.Abstractions;
using CadsBridge.Infrastructure.Storage.Clients;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace CadsBridge.Infrastructure.DataLoad.Csv.Strategies;

public class CsvDataFileSplitterStrategyNone(
    IS3ClientFactory s3ClientFactory,
    IFileEncryptor fileEncryptor,
    IOptions<EncryptionConfig> encryptionOptions,
    ILogger<CsvDataFileSplitterStrategyNone> logger) :
    CsvDataFileSplitterStrategy(fileEncryptor, encryptionOptions), ICsvDataFileSplitterStrategy
{
    public SplitType SplitType => SplitType.None;

    public async Task<long> ProcessAsync(CsvDataFileSplitJob job, CancellationToken cancellationToken)
    {
        if (logger.IsEnabled(LogLevel.Information))
            logger.LogInformation("Copying {Key} without splitting", job.SourceKey);

        var internalS3Info = s3ClientFactory.GetClientInfo<InternalStorageClient>();
        var s3 = internalS3Info.Client;
        using var response = await s3.GetObjectAsync(
            new GetObjectRequest { BucketName = internalS3Info.BucketName, Key = job.SourceKey },
            cancellationToken);

        await using var memoryStream = new MemoryStream();

        await response.ResponseStream.CopyToAsync(memoryStream, cancellationToken);

        Stream uploadStream = memoryStream;

        try
        {
            var uploadedLength = uploadStream.Length;

            await UploadAsync(s3,
                internalS3Info.BucketName,
                job.SourceKey.FormatSplitFileTargetKey(),
                uploadStream,
                cancellationToken: cancellationToken);

            return uploadedLength;
        }
        finally
        {
            await uploadStream.DisposeAsync();
        }
    }
}