using CadsBridge.Core.DTOs.SqsAdmin;

namespace CadsBridge.Endpoints.SqsAdmin.Responses;

public record GetQueueMetricsResponse(string Queue, QueueMetricsDto Metrics);