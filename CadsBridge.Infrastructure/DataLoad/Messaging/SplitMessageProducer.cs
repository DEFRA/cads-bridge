using CadsBridge.Application.DataLoad.Jobs;
using CadsBridge.Application.DataLoad.Messaging;
using Microsoft.Extensions.Logging;
using System.Threading.Channels;

namespace CadsBridge.Infrastructure.DataLoad.Messaging;

public class SplitMessageProducer(Channel<CsvDataFileSplitJob> channel, ILogger<SplitMessageProducer> logger) : ISplitMessageProducer
{
    public async ValueTask SendAsync(CsvDataFileSplitJob historicDataFileSplitJob, CancellationToken cancellationToken = default)
    {
        await channel.Writer.WriteAsync(historicDataFileSplitJob, cancellationToken);

        if (logger.IsEnabled(LogLevel.Information))
            logger.LogInformation("File split: {Key}, Split type: {SplitType}, Split size: {SplitSize}", historicDataFileSplitJob.Key, historicDataFileSplitJob.SplitType.ToString(), historicDataFileSplitJob.SplitValue.GetValueOrDefault());
    }
}