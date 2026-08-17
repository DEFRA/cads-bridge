using CadsBridge.Application.DataLoad.Csv.Abstractions;
using CadsBridge.Application.DataLoad.Jobs;
using CadsBridge.Core.Exceptions;
using CadsBridge.Infrastructure.DataLoad.Configuration;
using Microsoft.Extensions.Logging;

namespace CadsBridge.Infrastructure.DataLoad.Csv.Services;

public class CsvDataFileSplitterService(
    ICsvDataFileSplitterStrategyFactory csvDataFileSplitterStrategyFactory,
    DataLoadConfiguration config,
    ILogger<CsvDataFileSplitterService> logger)
    : ICsvDataFileSplitterService
{
    public async Task<long> ExecuteAsync(CsvDataFileSplitJob job, CancellationToken cancellationToken)
    {
        var attempt = 0;
        var csvDataFileSplitterStrategy = csvDataFileSplitterStrategyFactory.GetStrategy(config.SplitType);
        while (true)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                if (logger.IsEnabled(LogLevel.Information))
                {
                    logger.LogInformation("Cancellation requested for {Key}, aborting split", job.SourceKey);
                }
                return 0;
            }

            attempt++;

            if (attempt > config.MaxRetryAttempts)
            {
                throw new RetriesExceededException($"Exceeded maximum retry attempts ({config.MaxRetryAttempts}) for splitting {job.SourceKey}");
            }

            try
            {
                return await SplitAsync(job, attempt, csvDataFileSplitterStrategy, cancellationToken);
            }
            catch (Exception ex) when (
                attempt < config.MaxRetryAttempts &&
                ex is not NonRetryableException &&
                ex is not OperationCanceledException)
            {
                var delay = TimeSpan.FromMilliseconds(config.RetryDelayBase * Math.Pow(2, attempt - 1));
                if (logger.IsEnabled(LogLevel.Warning))
                {
                    logger.LogWarning(
                        ex,
                        "Error splitting {Key}, attempt {Attempt}/{Max}. Retrying in {Delay}ms",
                        job.SourceKey,
                        attempt,
                        config.MaxRetryAttempts,
                        delay.TotalMilliseconds);
                }

                await Task.Delay(delay, cancellationToken);
            }
        }
    }

    private async Task<long> SplitAsync(CsvDataFileSplitJob job, int attempt,
        ICsvDataFileSplitterStrategy csvDataFileSplitterStrategy, CancellationToken cancellationToken)
    {
        if (logger.IsEnabled(LogLevel.Information))
        {
            logger.LogInformation(
                "S3 splitting copy of {Key}, attempt {Attempt}",
                job.SourceKey,
                attempt);
        }

        var result = await csvDataFileSplitterStrategy.ProcessAsync(job, cancellationToken);

        if (logger.IsEnabled(LogLevel.Information))
        {
            logger.LogInformation(
                "S3 file split complete: {SourceKey}",
                job.SourceKey);
        }

        return result;
    }
}