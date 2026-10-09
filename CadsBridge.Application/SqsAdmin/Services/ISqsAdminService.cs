using CadsBridge.Core.DTOs.SqsAdmin;

namespace CadsBridge.Application.SqsAdmin.Services;

public interface ISqsAdminService
{
    Task<IReadOnlyList<QueueInfoDto>> GetQueuesAsync(CancellationToken cancellationToken = default);

    Task<QueueMetricsDto> GetMetricsAsync(string queue, CancellationToken cancellationToken = default);

    Task<IReadOnlyList<QueueMessageDto>> PeekMessagesAsync(PeekMessagesRequestDto request, CancellationToken cancellationToken = default);

    Task<QueueMetricsDto> GetDlqMetricsAsync(string queue, CancellationToken cancellationToken = default);

    Task<IReadOnlyList<QueueMessageDto>> PeekDlqMessagesAsync(PeekMessagesRequestDto request, CancellationToken cancellationToken = default);

    Task<ReplayResultDto> ReplayDlqAsync(ReplayDlqRequestDto request, CancellationToken cancellationToken = default);
}