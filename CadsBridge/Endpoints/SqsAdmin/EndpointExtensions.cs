using CadsBridge.Application.SqsAdmin.Services;
using CadsBridge.Core.DTOs.SqsAdmin;
using CadsBridge.Endpoints.SqsAdmin.Requests;
using CadsBridge.Endpoints.SqsAdmin.Responses;
using CadsBridge.Infrastructure.Authentication.Configuration;
using FluentValidation;
using Microsoft.AspNetCore.Mvc;

namespace CadsBridge.Endpoints.SqsAdmin;

public static class EndpointExtensions
{
    public static void CreateSqsAdminEndpoints(this IEndpointRouteBuilder app)
    {
        app.MapGet($"{SqsAdminEndpointsConstants.ApiRoutePrefix}/queues", GetQueues)
            .RequireAuthorization(AuthenticationConstants.AadSqsAdminExecutePolicy);

        app.MapGet($"{SqsAdminEndpointsConstants.ApiRoutePrefix}/queues/{{queue}}/metrics", GetMetrics)
            .RequireAuthorization(AuthenticationConstants.AadSqsAdminExecutePolicy);

        app.MapGet($"{SqsAdminEndpointsConstants.ApiRoutePrefix}/queues/{{queue}}/messages", GetMessages)
            .RequireAuthorization(AuthenticationConstants.AadSqsAdminExecutePolicy);

        app.MapGet($"{SqsAdminEndpointsConstants.ApiRoutePrefix}/queues/{{queue}}/dlq/messages", GetDlqMessages)
            .RequireAuthorization(AuthenticationConstants.AadSqsAdminExecutePolicy);

        app.MapGet($"{SqsAdminEndpointsConstants.ApiRoutePrefix}/queues/{{queue}}/dlq/metrics", GetDlqMetrics)
            .RequireAuthorization(AuthenticationConstants.AadSqsAdminExecutePolicy);

        app.MapPost($"{SqsAdminEndpointsConstants.ApiRoutePrefix}/queues/{{queue}}/dlq/replay", ReplayMessagesToQueue)
            .RequireAuthorization(AuthenticationConstants.AadSqsAdminExecutePolicy);
    }

    private static async Task<IResult> GetQueues(
        ISqsAdminService service,
        ILogger<GetQueuesResponse> logger,
        CancellationToken cancellationToken)
    {
        if (logger.IsEnabled(LogLevel.Information))
        {
            logger.LogInformation("[API SqsAdminEndpoint]: Getting queues");
        }
        var queues = await service.GetQueuesAsync(cancellationToken);
        if (logger.IsEnabled(LogLevel.Information))
        {
            logger.LogInformation("[API SqsAdminEndpoint]: Retrieved {QueueCount} queues", queues.Count);
        }
        return Results.Ok(new GetQueuesResponse(queues));
    }

        private static async Task<IResult> GetMetrics(
        [FromRoute] string queue,
        ISqsAdminService service,
        ILogger<GetQueuesResponse> logger,
        CancellationToken cancellationToken)
    {
        if (logger.IsEnabled(LogLevel.Information))
        {
            logger.LogInformation("[API SqsAdminEndpoint]: Getting metrics for queue {Queue}", queue);
        }
        var metrics = await service.GetMetricsAsync(queue, cancellationToken);
        if (logger.IsEnabled(LogLevel.Information))
        {
            logger.LogInformation("[API SqsAdminEndpoint]: Retrieved metrics for queue {Queue}", queue);
        }
        return Results.Ok(new GetQueueMetricsResponse(queue, metrics));
    }

    private static async Task<IResult> GetDlqMetrics(
        [FromRoute] string queue,
        ISqsAdminService service,
        ILogger<GetQueuesResponse> logger,
        CancellationToken cancellationToken)
    {
        if (logger.IsEnabled(LogLevel.Information))
        {
            logger.LogInformation("[API SqsAdminEndpoint]: Getting DLQ metrics for queue {Queue}", queue);
        }
        var metrics = await service.GetDlqMetricsAsync(queue, cancellationToken);
        if (logger.IsEnabled(LogLevel.Information))
        {
            logger.LogInformation("[API SqsAdminEndpoint]: Retrieved DLQ metrics for queue {Queue}", queue);
        }
        return Results.Ok(new GetQueueMetricsResponse(queue, metrics));
    }

    private static async Task<IResult> GetDlqMessages(
        [FromRoute] string queue,
        ISqsAdminService service,
        ILogger<GetQueuesResponse> logger,
        CancellationToken cancellationToken,
        [FromQuery] int maxMessages = 0)
    {
        if (logger.IsEnabled(LogLevel.Information))
        {
            logger.LogInformation("[API SqsAdminEndpoint]: Getting DLQ messages for queue {Queue}", queue);
        }
        var request = new PeekMessagesRequestDto(
            queue,
            maxMessages > 0 ? maxMessages : 10);

        var messages = await service.PeekDlqMessagesAsync(request, cancellationToken);
        if (logger.IsEnabled(LogLevel.Information))
        {
            logger.LogInformation("[API SqsAdminEndpoint]: Retrieved {MessageCount} DLQ messages for queue {Queue}", messages.Count, queue);
        }
        return Results.Ok(new GetQueueMessagesResponse(queue, messages));
    }

    private static async Task<IResult> GetMessages(
        [FromRoute] string queue,
        ISqsAdminService service,
        ILogger<GetQueuesResponse> logger,
        CancellationToken cancellationToken,
        [FromQuery] int maxMessages = 0)
    {
        if (logger.IsEnabled(LogLevel.Information))
        {
            logger.LogInformation("[API SqsAdminEndpoint]: Getting messages for queue {Queue}", queue);
        }
        var request = new PeekMessagesRequestDto(
            queue,
            maxMessages > 0 ? maxMessages : 10);

        var messages = await service.PeekMessagesAsync(request, cancellationToken);
        if (logger.IsEnabled(LogLevel.Information))
        {
            logger.LogInformation("[API SqsAdminEndpoint]: Retrieved {MessageCount} messages for queue {Queue}", messages.Count, queue);
        }
        return Results.Ok(new GetQueueMessagesResponse(queue, messages));
    }

    private static async Task<IResult> ReplayMessagesToQueue(
        [FromRoute] string queue,
        ReplayDlqRequest request,
        IValidator<ReplayDlqRequest> validator,
        ISqsAdminService service,
        HttpContext httpContext,
        ILogger<ReplayDlqRequest> logger,
        CancellationToken cancellationToken)
    {
        await validator.ValidateAndThrowAsync(request, cancellationToken);

        if (logger.IsEnabled(LogLevel.Information))
        {
            var user = httpContext.User.Identity!.Name;
            logger.LogInformation(
                "User {User}: Replaying DLQ messages for queue {Queue} with BatchSize: {BatchSize}",
                user, queue, request.BatchSize);
        }

        var result = await service.ReplayDlqAsync(
            new ReplayDlqRequestDto(queue, request.BatchSize ?? 0),
            cancellationToken);

        return Results.Ok(new ReplayDlqResponse(queue, result.Moved, result.Failed, result.Errors));
    }
}