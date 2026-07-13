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
        ILogger<Program> logger,
        CancellationToken cancellationToken)
    {
        var jobId = Guid.NewGuid().ToString("N");

        progressStore.InitJob(jobId, request.Files.Count);

        foreach (var importFile in request.Files)
        {
            try
            {
                await channel.Writer.WriteAsync(new CsvDataFileImportJob(
                    JobId: jobId,
                    SourceKey: importFile.sourceKey
                ), cancellationToken);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Failed to initiate import for {SourceKey} (job {JobId})",
                    importFile.sourceKey, jobId);
                progressStore.MarkFailed(jobId, importFile.sourceKey, ex.Message);
            }
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
            var targetFolder = $"import/{Path.GetFileNameWithoutExtension(file.Key)}";

            await channel.Writer.WriteAsync(new CsvDataFileSplitJob(
                JobId: jobId,
                SourceKey: file.Key
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