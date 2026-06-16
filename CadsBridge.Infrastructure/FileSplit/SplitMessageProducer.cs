using System.Threading.Channels;
using CadsBridge.Application.Models;
using Microsoft.Extensions.Logging;

namespace CadsBridge.Infrastructure.FileSplit;

public interface ISplitMessageProducer
{
    ValueTask SendAsync(HistoricDataFileSplitJob historicDataFileSplitJob, CancellationToken cancellationToken = default);
}

public class SplitMessageProducer(Channel<HistoricDataFileSplitJob> channel, ILogger<SplitMessageProducer> logger) : ISplitMessageProducer
{
    public async ValueTask SendAsync(HistoricDataFileSplitJob historicDataFileSplitJob, CancellationToken cancellationToken = default)
    {
        await channel.Writer.WriteAsync(historicDataFileSplitJob, cancellationToken);

        if (logger.IsEnabled(LogLevel.Information))
            logger.LogInformation("File split: {Key}, Split type: {SplitType}, Split size: {SplitSize}", historicDataFileSplitJob.Key, historicDataFileSplitJob.SplitType.ToString(), historicDataFileSplitJob.SplitValue.GetValueOrDefault());
    }
}