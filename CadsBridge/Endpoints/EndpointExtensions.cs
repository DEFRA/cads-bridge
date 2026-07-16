using CadsBridge.Application.DataLoad.Jobs;
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

        app.MapPost("split", Split);

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
        ILogger<Program> logger,
        CancellationToken cancellationToken)
    {
        foreach (var sourceKey in request.Files.Select(importFile => importFile.sourceKey))
        {
            try
            {
                await channel.Writer.WriteAsync(new CsvDataFileImportJob(
                    SourceKey: sourceKey
                ), cancellationToken);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Failed to initiate import for {SourceKey}",
                    sourceKey);
            }
        }

        return Results.Ok();
    }

    private static async Task<IResult> Split([FromBody] CsvDataFileSplitRequest request, Channel<CsvDataFileSplitJob> channel)
    {
        foreach (var file in request.Files)
        {
            await channel.Writer.WriteAsync(new CsvDataFileSplitJob(file.Key));
        }

        return Results.Ok();
    }
}