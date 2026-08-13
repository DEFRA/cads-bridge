using CadsBridge.Core.ApiClients;
using CadsBridge.Infrastructure.ApiClients.Contracts;
using CadsBridge.Infrastructure.ApiClients.DTOs;
using CadsBridge.Infrastructure.ApiClients.DTOs.Requests;
using System.Diagnostics.CodeAnalysis;

namespace CadsBridge.Infrastructure.ApiClients.Fakes;

/// <summary>
/// In-process stub for <see cref="IFileImportApiService"/> used when
/// <c>ApiClients:CdsApi:UseFakeClient</c> is <c>true</c>.
///
/// <para>
/// The static lookup table mirrors the rows inserted by the
/// <c>0003_026_integration_cts_file_imports_seed_data</c> liquibase changeset so that
/// integration tests can run without a live cads-data-service instance.
/// Any file name not present in the table is treated as "not yet seen" (returns
/// <c>null</c>), which causes <see cref="CadsBridge.Infrastructure.DataLoad.Services.S3FileDiscoveryService{TClient}"/>
/// to enqueue it for processing.
/// </para>
/// </summary>
[ExcludeFromCodeCoverage]
public class FakeFileImportApiService : IFileImportApiService
{
    private readonly Random _random = new();

    // ── Seed data from 0003_026_integration_cts_file_imports_seed_data ──────────
    // Key   = file_name column value (no file extension – matches the S3 object key).
    // Value = (ImportStatus, FailedAttempts)
    private static readonly Dictionary<string, (FileImportStatus Status, int FailedAttempts)> s_seedData =
        new(StringComparer.OrdinalIgnoreCase)
        {
            // ImportStatus 4 = Completed  → IsFileValid returns false  → not re-enqueued
            ["CTSM_CADS_PROD_BULK_ABC_0004_CT_PARTIES_2026-01-01-012345.csv"] = (FileImportStatus.Completed, 0),

            // ImportStatus 5 = Failed, 0 prior attempts → IsFileValid returns true → re-enqueued
            ["CTSM_CADS_PROD_BULK_ABC_0005_CT_PARTIES_2026-01-01-012345.csv"] = (FileImportStatus.Failed, 0),

            // ImportStatus 4 = Completed  → IsFileValid returns false  → not re-enqueued
            ["CTSM_CADS_PROD_DELTA_ABC_0004_CT_PARTIES_2026-01-01-012345.csv"] = (FileImportStatus.Completed, 0),

            // ImportStatus 5 = Failed, 0 prior attempts → IsFileValid returns true → re-enqueued
            ["CTSM_CADS_PROD_DELTA_ABC_0005_CT_PARTIES_2026-01-01-012345.csv"] = (FileImportStatus.Failed, 0)
        };

    public Task<FileImportDto?> GetByFileNameIfExists(string objectKey, CancellationToken cancellationToken)
    {
        if (!s_seedData.TryGetValue(objectKey, out var entry))
        {
            // File not in seed data → treat as new / not yet seen
            return Task.FromResult<FileImportDto?>(null);
        }

        var dto = new FileImportDto(entry.FailedAttempts)
        {
            Id = _random.Next(1, 99),
            FileName = objectKey,
            ImportStatus = entry.Status,
            ProcessingStatus = FileProcessingStatus.Pending,
            TotalRowsToProcess = 0,
            RowsFound = 0
        };
        return Task.FromResult<FileImportDto?>(dto);
    }


    public Task<FileImportDto?> GetByFileName(string objectKey, CancellationToken cancellationToken)
    {
        var response = new FileImportDto
        {
            Id = _random.Next(1, 99),
            FileName = objectKey,
            ImportStatus = FileImportStatus.Pending,
            ProcessingStatus = FileProcessingStatus.Pending,
            TotalRowsToProcess = 0,
            RowsFound = 0
        };
        return Task.FromResult<FileImportDto?>(response);
    }

    public Task<FileImport> Create(string objectKey, long totalRowsToProcess, CancellationToken cancellationToken)
    {
        return Task.FromResult(new FileImport
        {
            Id = _random.Next(1, 99),
            FileName = objectKey,
            ImportStatus = FileImportStatus.Pending,
            DestinationTableName = "cts_table",
            FailedAttempts = 0
        });
    }

    public Task Update(long id, UpdateFileImportRequest request, CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }

    public Task MarkStatus(long id, FileImportStatus status, CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }

    public Task MarkFailed(long id, string reason, CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }

    public Task MarkReset(long id, CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }
}