using CadsBridge.Core.DTOs.SqsAdmin;

namespace CadsBridge.Endpoints.SqsAdmin.Responses;

public record GetQueuesResponse(IReadOnlyList<QueueInfoDto> Queues);