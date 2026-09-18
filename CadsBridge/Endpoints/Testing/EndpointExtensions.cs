using CadsBridge.Core.Exceptions;
using CadsBridge.Endpoints.Testing.Models;
using FluentValidation;
using Microsoft.AspNetCore.Mvc;
using System.Text;

namespace CadsBridge.Endpoints.Testing;

public static class EndpointExtensions
{
    private const int MaxContentSizeBytes = 64 * 1024;

    public static void CreateTestEndpoints(this IEndpointRouteBuilder app)
    {
        app.MapPost("/api/v1/test-support/files", UploadTestFiles);
    }

    private static async Task<IResult> UploadTestFiles(
        [FromBody] CreateTestFileRequest request,
        IValidator<CreateTestFileRequest> validator,
        CancellationToken cancellationToken)
    {
        await validator.ValidateAndThrowAsync(request, cancellationToken);

        if (Encoding.UTF8.GetByteCount(request.Content) > MaxContentSizeBytes)
        {
            throw new PayloadTooLargeException(
                $"Content exceeds the maximum allowed size of {MaxContentSizeBytes} bytes for test file uploads.");
        }

        // Implementation for uploading test files
        return Results.Ok();
    }
}