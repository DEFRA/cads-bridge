using System.Threading.Channels;
using CadsBridge.Application.Models;
using Microsoft.Extensions.Logging;

namespace CadsBridge.Infrastructure.FileSplit;

public interface ISplitMessageProducer
{
    ValueTask SendAsync(FileSplitJob fileSplitJob, CancellationToken cancellationToken = default);
}

public class SplitMessageProducer(Channel<FileSplitJob> channel, ILogger<SplitMessageProducer> logger) : ISplitMessageProducer
{
    public async ValueTask SendAsync(FileSplitJob fileSplitJob, CancellationToken cancellationToken = default)
    {
        await channel.Writer.WriteAsync(fileSplitJob, cancellationToken);

        if (logger.IsEnabled(LogLevel.Information))
            logger.LogInformation("File split: {Key}, Split type: {SplitType}, Split size: {SplitSize}", fileSplitJob.Key, fileSplitJob.SplitType.ToString(), fileSplitJob.SplitValue.GetValueOrDefault());
    }
}