using CadsBridge.Application.DataLoad.Jobs;
using CadsBridge.Application.DataLoad.Services;
using CadsBridge.Core.Exceptions;
using CadsBridge.Infrastructure.Storage.Abstractions;
using CadsBridge.Infrastructure.Storage.Clients;
using Microsoft.Extensions.Logging;
using CadsBridge.Infrastructure.DataLoad.Csv.Contracts;

namespace CadsBridge.Infrastructure.DataLoad.Services;

public class CsvDataFileSplitterService(
    IS3ClientFactory s3ClientFactory,
    ICsvDataFileSplitterStrategyFactory csvDataFileSplitterStrategyFactory,
    ILogger<CsvDataFileSplitterService> logger)
    : ICsvDataFileSplitterService
{
    private readonly int _maxRetries = 3;
    private readonly int _delayBaseMs = 500;

    public async Task<bool> ExecuteAsync(CsvDataFileSplitJob job, CancellationToken cancellationToken)
    {

        var attempt = 0;
        var internalS3Info = s3ClientFactory.GetClientInfo<InternalStorageClient>();
        while (true)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                if (logger.IsEnabled(LogLevel.Information))
                    logger.LogInformation("Cancellation requested for {Key}, aborting split", job.Key);
                return false;
            }

            attempt++;
            if (attempt > _maxRetries)
            {
                throw new RetriesExceededException($"Exceeded maximum retry attempts ({_maxRetries}) for splitting {job.Key}");
            }

            try
            {
                if (logger.IsEnabled(LogLevel.Information))
                    logger.LogInformation(
                        "S3 splitting copy of {Key} from {SourceBucket}, attempt {Attempt}",
                        job.Key,
                        internalS3Info.BucketName,
                        attempt);

                var csvDataFileSplitterStrategy = csvDataFileSplitterStrategyFactory.GetStrategy(job.SplitType);
                await csvDataFileSplitterStrategy.Process(job, internalS3Info, logger, cancellationToken);

                if (logger.IsEnabled(LogLevel.Information))
                    logger.LogInformation(
                        "S3 file split complete: {SourceBucket}/{SourceKey}",
                        internalS3Info.BucketName,
                        job.Key);

                return true;
            }
            catch (Exception ex) when (attempt < _maxRetries)
            {
                var delay = TimeSpan.FromMilliseconds(_delayBaseMs * Math.Pow(2, attempt - 1));

                logger.LogWarning(
                    ex,
                    "Error splitting {Key}, attempt {Attempt}/{Max}. Retrying in {Delay}ms",
                    job.Key,
                    attempt,
                    _maxRetries,
                    delay.TotalMilliseconds);

                await Task.Delay(delay, cancellationToken);
            }
        }
    }
}