using CadsBridge.Application.Models;
using Microsoft.Extensions.Logging;
using System.Threading.Channels;

namespace CadsBridge.Application.Services;

public interface ISplitMessageProducer
{
    ValueTask ProduceAsync(FileSplitJob fileSplitJob, CancellationToken cancellationToken = default);
}

internal class SplitMessageProducer : ISplitMessageProducer
{

    private readonly Channel<FileSplitJob> _channel;
    private readonly ILogger<SplitMessageProducer> _logger;

    public SplitMessageProducer(Channel<FileSplitJob> channel, ILogger<SplitMessageProducer> logger)
    {
        _channel = channel;
        _logger = logger;
    }

    public async ValueTask ProduceAsync(FileSplitJob fileSplitJob, CancellationToken cancellationToken = default)
    {
        await _channel.Writer.WriteAsync(fileSplitJob, cancellationToken);
        _logger.LogInformation("File split: {Key}, Split type: {SplitType}, Split size: {SplitSize}", fileSplitJob.Key, fileSplitJob.SplitType.ToString(), fileSplitJob.SplitValue.GetValueOrDefault());
    }
}
