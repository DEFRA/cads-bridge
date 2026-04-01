using CadsBridge.Application.Models;
using CadsBridge.Application.Persistance;
using Microsoft.AspNetCore.Mvc;
using System.Diagnostics.CodeAnalysis;
using System.Threading.Channels;

namespace CadsBridge.Endpoints;

[ExcludeFromCodeCoverage]
public static class EndpointsExtensions
{
    public static void CreateEndpoints(this IEndpointRouteBuilder app)
    {
        app.MapPost("start", Start);

        app.MapGet("progress/{jobId}", GetProgress);
    }

    private static async Task<IResult> Start([FromBody] StartImportRequest request, Channel<FileImportJob> channel, IImportProgressStore progressStore)
    {
        var jobId = Guid.NewGuid().ToString("N");

        progressStore.InitJob(jobId, request.Files.Count);

        foreach (var importFile in request.Files)
        {
            await channel.Writer.WriteAsync(new FileImportJob(
                JobId: jobId,
                SourceKey: importFile.sourceKey,
                TargetKey: importFile.targetKey,
                Password: importFile.Password,
                Salt: importFile.Salt
            ));
        }

        return Results.Ok(new { jobId });
    }

    private static async Task<IResult> GetProgress(string jobId, Channel<FileImportJob> channel, IImportProgressStore progressStore)
    {
        var job = progressStore.GetJob(jobId);
        if (job is null) return Results.NotFound();

        return Results.Ok(job);
    }
}