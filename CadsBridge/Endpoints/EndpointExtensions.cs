using CadsBridge.Application.DataLoad.Jobs;
using CadsBridge.Application.DataLoad.Persistence;
using CadsBridge.Application.DataLoad.Services;
using CadsBridge.Endpoints.Requests;
using Microsoft.AspNetCore.Mvc;
using System.Threading.Channels;

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

    private static async Task<IResult> GetDataSeed(Channel<DataSeedFileLoadJob> channel, IConfiguration configuration, IDataSeedFileLoadService dataSeedFileLoader)
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
            await channel.Writer.WriteAsync(new DataSeedFileLoadJob(
                JobId: jobId,
                FileName: file.FilePath,
                TargetKey: $"data-seed/{file.FileName}"
            ));
        }

        return Results.Ok(new { fileCount = files.Count, files = files.Select(f => f.FileName) });
    }

    private static async Task<IResult> Import(
        [FromBody] CsvDataFileImportRequest request,
        Channel<CsvDataFileImportJob> channel,
        IImportJobProgressStore progressStore,
        IS3FileMetaDataService s3FileMetaDataService,
        IFileImportStatusStore fileImportStatusStore)
    {
        var jobId = Guid.NewGuid().ToString("N");

        progressStore.InitJob(jobId, request.Files.Count);

        foreach (var importFile in request.Files)
        {
            // TODO: Handle errors
            var totalRowsToProcess = 0;
            // var totalRowsToProcess = await s3FileMetaDataService.GetRecordCountAsync(importFile.sourceKey);
            // var id = await fileImportStatusStore.Initiate(importFile.sourceKey, totalRowsToProcess);
            await channel.Writer.WriteAsync(new CsvDataFileImportJob(
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

    private static async Task<IResult> Split([FromBody] CsvDataFileSplitRequest request, Channel<CsvDataFileSplitJob> channel, ISplitJobProgressStore progressStore)
    {
        var jobId = Guid.NewGuid().ToString("N");

        progressStore.InitJob(jobId, request.Files.Count);

        foreach (var file in request.Files)
        {
            await channel.Writer.WriteAsync(new CsvDataFileSplitJob(
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