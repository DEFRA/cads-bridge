using CadsBridge.Application.DataLoad.Services.Testing;
using CadsBridge.Core.Exceptions;
using CadsBridge.Endpoints.Testing.Models;
using CadsBridge.Infrastructure.Authentication.Configuration;
using FluentValidation;
using Microsoft.AspNetCore.Mvc;
using System.Text;
using CadsBridge.Endpoints.Testing.Validators;

namespace CadsBridge.Endpoints.Testing;

public static class EndpointExtensions
{
    private const int MaxContentSizeBytes = 64 * 1024;

    public static void CreateTestSupportEndpoints(this IEndpointRouteBuilder app)
    {
        app.MapPost("/api/v1/test-support/files", UploadTestFiles)
            .RequireAuthorization(AuthenticationConstants.DefaultPolicy);

        app.MapGet("/api/v1/test-support/internal-files", GetInternalFiles)
            .RequireAuthorization(AuthenticationConstants.DefaultPolicy);
    }

    private static async Task<IResult> GetInternalFiles(
        [FromQuery] string? fileName,
        IInternalFileService service,
        GetInternalFilesRequestValidator validator,
        CancellationToken cancellationToken)
    {
        var request = new  GetInternalFilesRequest(fileName);
        var validationResult = await validator.ValidateAsync(request, cancellationToken);

        if (!validationResult.IsValid)
        {
            var validationErrors = validationResult.ToDictionary();
            return Results.ValidationProblem(validationErrors);
        }

        var chunks = await service.ListChunksAsync(fileName ?? string.Empty, cancellationToken);

        var response = new GetInternalFilesResponse(
            fileName!,
            [.. chunks.Select(c => new InternalFileChunkResponse(c.Key, c.ChunkNo, c.SizeBytes, c.RecordCount))]);

        return Results.Ok(response);
    }

    private static async Task<IResult> UploadTestFiles(
        [FromBody] CreateTestFileRequest request,
        IValidator<CreateTestFileRequest> validator,
        IS3UploadService s3UploadService,
        CancellationToken cancellationToken)
    {
        await validator.ValidateAndThrowAsync(request, cancellationToken);

        if (Encoding.UTF8.GetByteCount(request.Content) > MaxContentSizeBytes)
        {
            throw new PayloadTooLargeException(
                $"Content exceeds the maximum allowed size of {MaxContentSizeBytes} bytes for test file uploads.");
        }

        var uploadDetails = await s3UploadService.UploadAsync(request.FileName, request.Content, request.DataSource, request.OverwriteExisting, cancellationToken);

        var result = new CreateTestFileResponse(request.FileName, uploadDetails.BucketName, uploadDetails.Key, uploadDetails.Size, uploadDetails.UploadTime);
        return Results.Created(uploadDetails.Key, result);
    }
}