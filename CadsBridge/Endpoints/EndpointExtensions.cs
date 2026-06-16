using CadsBridge.Application.Models;
using CadsBridge.Application.Persistence;
using Microsoft.AspNetCore.Mvc;
using System.Diagnostics.CodeAnalysis;
using System.Threading.Channels;
using CadsBridge.Core.DataSeed.Abstractions;

namespace CadsBridge.Endpoints;

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

        app.MapGet("data-seed/import", GetDataSeed);
    }

    private static async Task<IResult> GetDataSeed(Channel<DataSeedImportJob> channel, IConfiguration configuration, IDataSeedFileLoader dataSeedFileLoader)
    {
        var dataSeedingImportEnabled = configuration.GetValue<bool>("DataSeedingImportEnabled");
        if (!dataSeedingImportEnabled)
        {
            return Results.Ok("Data seeding import is disabled.");
        }

        var files = dataSeedFileLoader.GetFiles();
        if (files.Count == 0)
        {
            return Results.Ok("No data seed files found.");
        }

        foreach (var file in files)
        {
            var jobId = Guid.NewGuid().ToString("N");
            await channel.Writer.WriteAsync(new DataSeedImportJob(
                JobId: jobId,
                FileName: file.FilePath,
                TargetKey: $"data-seed/{file.FileName}"
            ));
        }

        return Results.Ok(new { fileCount = files.Count, files = files.Select(f => f.FileName) });
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
                SplitType: importFile.SplitType,
                SplitValue: importFile.SplitValue
            ));
        }

        return Results.Ok(new { jobId });
    }

    private static async Task<IResult> GetImportProgress(string jobId, IImportJobProgressStore progressStore)
    {
        if (!string.IsNullOrEmpty(jobId))
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
                SplitType: file.SplitType,
                SplitValue: file.SplitValue
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