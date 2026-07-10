using CadsBridge.Application.DataLoad.Csv.Abstractions;
using CadsBridge.Application.DataLoad.Jobs;
using CadsBridge.Core.Exceptions;
using Microsoft.Extensions.Logging;

namespace CadsBridge.Infrastructure.DataLoad.Csv.Services;

public class CsvDataFileSplitterService(
    ICsvDataFileSplitterStrategyFactory csvDataFileSplitterStrategyFactory,
    ILogger<CsvDataFileSplitterService> logger)
    : ICsvDataFileSplitterService
{
    private readonly int _maxRetries = 3;
    private readonly int _delayBaseMs = 500;

    public async Task<bool> ExecuteAsync(CsvDataFileSplitJob job, CancellationToken cancellationToken)
    {

        var attempt = 0;
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
                        "S3 splitting copy of {Key}, attempt {Attempt}",
                        job.Key,
                        attempt);

                var csvDataFileSplitterStrategy = csvDataFileSplitterStrategyFactory.GetStrategy(job.SplitType);
                await csvDataFileSplitterStrategy.Process(job, cancellationToken);

                if (logger.IsEnabled(LogLevel.Information))
                    logger.LogInformation(
                        "S3 file split complete: {SourceKey}",
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