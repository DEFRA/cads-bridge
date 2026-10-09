using CadsBridge.Core.DTOs.SqsAdmin;

namespace CadsBridge.Endpoints.SqsAdmin.Responses;

public record GetQueueMessagesResponse(string Queue, IReadOnlyList<QueueMessageDto> Messages);