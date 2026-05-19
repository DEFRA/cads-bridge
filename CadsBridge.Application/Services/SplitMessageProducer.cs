using CadsBridge.Application.Models;
using Microsoft.Extensions.Logging;
using System.Threading.Channels;

namespace CadsBridge.Application.Services;

public interface ISplitMessageProducer
{
    ValueTask SendAsync(FileSplitJob fileSplitJob, CancellationToken cancellationToken = default);
}

internal class SplitMessageProducer(Channel<FileSplitJob> channel, ILogger<SplitMessageProducer> logger) : ISplitMessageProducer
{
    public async ValueTask SendAsync(FileSplitJob fileSplitJob, CancellationToken cancellationToken = default)
    {
        await channel.Writer.WriteAsync(fileSplitJob, cancellationToken);
        logger.LogInformation("File split: {Key}, Split type: {SplitType}, Split size: {SplitSize}", fileSplitJob.Key, fileSplitJob.SplitType.ToString(), fileSplitJob.SplitValue.GetValueOrDefault());
    }
}