using Amazon.S3.Model;
using CadsBridge.Application.DataLoad.Csv.Abstractions;
using CadsBridge.Application.DataLoad.Jobs;
using CadsBridge.Core.DataLoad.Jobs;
using CadsBridge.Infrastructure.DataLoad.Configuration;
using CadsBridge.Infrastructure.DataLoad.Csv.Extensions;
using CadsBridge.Infrastructure.Storage.Abstractions;
using CadsBridge.Infrastructure.Storage.Clients;
using Microsoft.Extensions.Logging;
using System.Text;

namespace CadsBridge.Infrastructure.DataLoad.Csv.Strategies;

public class CsvDataFileSplitterStrategyByLines(
    IS3ClientFactory s3ClientFactory,
    DataLoadConfiguration config,
    ILogger<CsvDataFileSplitterStrategyByLines> logger) :
    ICsvDataFileSplitterStrategy
{
    public SplitType SplitType => SplitType.ByLines;

    public async Task Process(CsvDataFileSplitJob job, CancellationToken cancellationToken)
    {
        if (!config.SplitValue.HasValue)
        {
            throw new ArgumentException("Split value must be specified for splitting.");
        }
        var internalS3Info = s3ClientFactory.GetClientInfo<InternalStorageClient>();
        var s3 = internalS3Info.Client;
        using var response = await s3.GetObjectAsync(
            new GetObjectRequest { BucketName = internalS3Info.BucketName, Key = job.SourceKey },
            cancellationToken);

        using var reader = new StreamReader(response.ResponseStream);

        // read the file header information, should be the first line in the file.
        // IGNORED: We are not using the header information in the splitting process,
        // but we read it to ensure we start processing from the correct line and
        // to maintain the structure of the CSV in the output chunks.
        var header = await reader.ReadLineAsync(cancellationToken);
        if (header is null)
        {
            return;
        }

        // read the column definitions, should be the second line in the file.
        var columns = await reader.ReadLineAsync(cancellationToken);
        if (columns is null)
        {
            return;
        }

        // Process the column definitions to remove the first column
        // and apply lowercase to the remaining columns.
        columns = columns.ProcessColumnDefinitions('|');

        var chunkNumber = 1;
        var lineCount = 0;
        var chunkBuilder = new StringBuilder();

        chunkBuilder.AppendLine(columns);

        while (await reader.ReadLineAsync(cancellationToken) is { } line)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                if (logger.IsEnabled(LogLevel.Information))
                    logger.LogInformation("Cancellation requested for {Key}, aborting split", job.SourceKey);
                return;
            }

            chunkBuilder.AppendLine(line);
            lineCount++;

            if (lineCount >= config.SplitValue)
            {
                await s3.UploadChunkAsync(
                    internalS3Info.BucketName,
                    job.SourceKey.FormatSplitFileTargetKey(chunkNumber),
                    chunkBuilder.ToString(),
                    cancellationToken: cancellationToken);

                chunkNumber++;
                lineCount = 0;
                chunkBuilder.Clear();

                chunkBuilder.AppendLine(columns);
            }
        }

        if (lineCount > 0)
        {
            await s3.UploadChunkAsync(
                internalS3Info.BucketName,
                job.SourceKey.FormatSplitFileTargetKey(chunkNumber),
                chunkBuilder.ToString(),
                cancellationToken: cancellationToken);
        }
    }
}