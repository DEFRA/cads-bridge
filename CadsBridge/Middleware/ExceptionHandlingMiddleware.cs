using CadsBridge.Core.Exceptions;
using CadsBridge.Infrastructure.Json;
using Microsoft.AspNetCore.Mvc;
using System.Text.Json;

namespace CadsBridge.Middleware;

public sealed class ExceptionHandlingMiddleware(
        RequestDelegate next,
        ILogger<ExceptionHandlingMiddleware> logger,
        IConfiguration cfg)
{
    private readonly RequestDelegate _next = next;
    private readonly ILogger<ExceptionHandlingMiddleware> _logger = logger;
    private readonly string _traceHeader = cfg.GetValue<string>("TraceHeader") ?? "x-correlation-id";

    public async Task InvokeAsync(HttpContext context)
    {
        context.Request.Headers.TryGetValue(_traceHeader, out var headerValues);
        var correlationId = headerValues.FirstOrDefault() ?? context.TraceIdentifier;

        try
        {
            await _next(context);
        }
        catch (FluentValidation.ValidationException ex)
        {
            await HandleExceptionAsync(context, ex, correlationId, 422, "Unprocessable Content");
        }
        catch (NotFoundException ex)
        {
            await HandleExceptionAsync(context, ex, correlationId, 404);
        }
        catch (DomainException ex)
        {
            await HandleExceptionAsync(context, ex, correlationId, 400);
        }
        catch (Exception ex)
        {
            await HandleExceptionAsync(context, ex, correlationId, 500, "An error occurred");
        }
    }

    private Task HandleExceptionAsync(
        HttpContext context,
        Exception exception,
        string correlationId,
        int statusCode,
        string? title = null)
    {
        var errorId = Guid.NewGuid().ToString();

        using (_logger.BeginScope(new Dictionary<string, object> { ["trace.id"] = correlationId, ["error.id"] = errorId }))
        {
            const string logMessageTemplate = "ErrorId: {errorId} | CorrelationId: {correlationId} | StatusCode: {statusCode}";

            if (statusCode == 500)
            {
                _logger.LogError(exception, logMessageTemplate, errorId, correlationId, statusCode);
            }
            else
            {
                if (_logger.IsEnabled(LogLevel.Information))
                {
                    _logger.LogInformation(exception, logMessageTemplate, errorId, correlationId, statusCode);
                }
            }
        }

        var resolvedTitle = title
            ?? (exception is DomainException de ? de.Title : "An error occurred");

        var problemDetails = new ProblemDetails
        {
            Title = resolvedTitle,
            Detail = exception.Message,
            Status = statusCode,
            Instance = context.Request.Path
        };

        if (exception is FluentValidation.ValidationException validationException)
        {
            var validationErrors = validationException.Errors
                .GroupBy(e => e.PropertyName)
                .ToDictionary(
                    g => g.Key,
                    g => g.Select(e => e.ErrorMessage).ToArray()
                );

            problemDetails.Extensions["errors"] = validationErrors;
            problemDetails.Detail = "One or more validation errors occurred. See the 'errors' field for details.";
        }

        problemDetails.Extensions["traceId"] = correlationId;
        problemDetails.Extensions["correlationId"] = correlationId;
        problemDetails.Extensions["errorId"] = errorId;

        context.Response.StatusCode = statusCode;
        context.Response.ContentType = "application/json";

        var json = JsonSerializer.Serialize(problemDetails, JsonDefaults.DefaultOptionsWithIndented);
        return context.Response.WriteAsync(json);
    }
}