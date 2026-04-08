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
        app.MapPost("import", Import);

        app.MapGet("import/{jobId}/progress", GetImportProgress);

        app.MapGet("import/progress", GetImportProgress);

        app.MapPost("split", Split);

        app.MapGet("split/{jobId}/progress", GetSplitProgress);

        app.MapGet("split/progress", GetSplitProgress);
    }

    private static async Task<IResult> Import([FromBody] ImportRequest request, Channel<FileImportJob> channel, IImportJobProgressStore progressStore)
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
                Salt: importFile.Salt, 
                SplitFileSizeInMBytes: importFile.SplitFileSizeInMBytes,
                SplitLinesPerFile: importFile.SplitLinesPerFile
            ));
        }

        return Results.Ok(new { jobId });
    }

    private static async Task<IResult> GetImportProgress(string jobId, IImportJobProgressStore progressStore)
    {
        if (string.IsNullOrEmpty(jobId))
        {
            var job = progressStore.GetJob(jobId);
            if (job is null) return Results.NotFound();

            return Results.Ok(job);
        }

        return Results.Ok(progressStore.GetJobs());
    }

    private static async Task<IResult> Split([FromBody] SplitRequest request, Channel<FileSplitJob> channel, ISplitJobProgressStore progressStore)
    {
        var jobId = Guid.NewGuid().ToString("N");

        progressStore.InitJob(jobId, request.Files.Count);

        foreach (var file in request.Files)
        {
            await channel.Writer.WriteAsync(new FileSplitJob(
                JobId: jobId,
                Key: file.Key,
                TargetFolder: file.TargetFolder,
                FileSizeInMBytes: file.FileSizeInMBytes,
                LinesPerFile: file.LinesPerFile
            ));
        }

        return Results.Ok(new { jobId });
    }

    private static async Task<IResult> GetSplitProgress(string jobId, ISplitJobProgressStore progressStore)
    {

        if (string.IsNullOrEmpty(jobId))
        {
            var job = progressStore.GetJob(jobId);
            if (job is null) return Results.NotFound();

            return Results.Ok(job);
        }

        
        return Results.Ok(progressStore.GetJobs());
    }
}