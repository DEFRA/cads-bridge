using Amazon.S3.Model;
using CadsBridge.Application.DataLoad.Csv.Abstractions;
using CadsBridge.Application.DataLoad.Jobs;
using CadsBridge.Core.DataLoad;
using CadsBridge.Infrastructure.DataLoad.Configuration;
using CadsBridge.Infrastructure.DataLoad.Csv.Extensions;
using CadsBridge.Infrastructure.Storage.Abstractions;
using CadsBridge.Infrastructure.Storage.Clients;
using Microsoft.Extensions.Logging;
using System.Text;
using CadsBridge.Core.Exceptions;

namespace CadsBridge.Infrastructure.DataLoad.Csv.Strategies;

public class CsvDataFileSplitterStrategyByLines(
    IS3ClientFactory s3ClientFactory,
    DataLoadConfiguration config,
    ILogger<CsvDataFileSplitterStrategyByLines> logger) :
    ICsvDataFileSplitterStrategy
{
    private const char ColumnDelimiter = '|';
    private const char TerminatorRowIndicator = 'T';

    public SplitType SplitType => SplitType.ByLines;

    public async Task<long> ProcessAsync(CsvDataFileSplitJob job, CancellationToken cancellationToken)
    {
        if (!config.SplitValue.HasValue)
        {
            throw new NonRetryableException("Split value must be specified for splitting.");
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
            return 0;
        }

        // read the column definitions, should be the second line in the file.
        var columns = await reader.ReadLineAsync(cancellationToken);
        if (columns is null)
        {
            return 0;
        }

        // Process the column definitions to remove the first column
        // and apply lowercase to the remaining columns.
        columns = columns.ProcessColumnDefinitions(ColumnDelimiter);

        var chunkNumber = 1;
        var lineCount = 0;
        var totalLinesProcessed = 0;
        var chunkBuilder = new StringBuilder();

        chunkBuilder.AppendLine(columns);

        while (await reader.ReadLineAsync(cancellationToken) is { } line)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                if (logger.IsEnabled(LogLevel.Information))
                    logger.LogInformation("Cancellation requested for {Key}, aborting split", job.SourceKey);
                return 0;
            }

            if (string.IsNullOrWhiteSpace(line))
            {
                continue; // Skip empty lines
            }

            var isTerminatorLine = line[0].Equals(TerminatorRowIndicator);
            // Exclude terminator line from processing.
            if (!isTerminatorLine)
            {
                chunkBuilder.AppendLine(line);
                lineCount++;
            }

            if (lineCount >= config.SplitValue || (lineCount > 0 && isTerminatorLine)) // only upload if there are lines to upload
            {
                await s3.UploadChunkAsync(
                    internalS3Info.BucketName,
                    job.SourceKey.FormatSplitFileTargetKey(chunkNumber),
                    chunkBuilder.ToString(),
                    cancellationToken: cancellationToken);

                chunkNumber++;
                totalLinesProcessed += lineCount;
                lineCount = 0;
                chunkBuilder.Clear();

                chunkBuilder.AppendLine(columns);
            }
        }

        if (lineCount > 0)
        {
            totalLinesProcessed += lineCount;

            await s3.UploadChunkAsync(
                internalS3Info.BucketName,
                job.SourceKey.FormatSplitFileTargetKey(chunkNumber),
                chunkBuilder.ToString(),
                cancellationToken: cancellationToken);
        }
        return totalLinesProcessed;
    }
}