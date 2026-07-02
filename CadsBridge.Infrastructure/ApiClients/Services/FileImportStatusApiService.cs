using System.Net.Http.Json;
using CadsBridge.Core.Exceptions;
using CadsBridge.Infrastructure.ApiClients.Configuration;
using CadsBridge.Infrastructure.ApiClients.Contracts;
using CadsBridge.Infrastructure.ApiClients.DTOs;
using CadsBridge.Infrastructure.ApiClients.DTOs.Requests;
using Microsoft.AspNetCore.Mvc;

namespace CadsBridge.Infrastructure.ApiClients.Services;


public class FileImportStatusApiService : IFileImportStatusApiService
{
    private readonly HttpClient _httpClient;
    private const string baseApiUrl = "api/v1/systemadmin/fileimports";
    private const string getByFileNameEndpoint = "/by-file-name";

    public FileImportStatusApiService(IHttpClientFactory httpClientFactory)
    {
        _httpClient = httpClientFactory.CreateClient(nameof(ApiClientNames.CdsApi));
    }

    public async Task<FileImportStatusDto?> GetByFileName(string fileName, CancellationToken cancellationToken)
    {
        var response = await _httpClient.GetAsync($"{baseApiUrl}{getByFileNameEndpoint}?fileName={fileName}", cancellationToken);

        response.EnsureSuccessStatusCode();

        var result = await response.Content.ReadFromJsonAsync<FileImportStatusDto>(cancellationToken);
        return result;
    }

    public async Task<FileImportStatusDto?> GetById(Guid id, CancellationToken cancellationToken)
    {
        // TODO: we don't have this endpoint in the CDS API yet
        var response = await _httpClient.GetAsync(
            $"/api/get-endpoint?id={id}",
            cancellationToken);

        response.EnsureSuccessStatusCode();

        var result = await response.Content.ReadFromJsonAsync<FileImportStatusDto>(cancellationToken);
        return result;
    }

    public async Task<long> Create(string s3Key, long recordCount, CancellationToken cancellationToken)
    {
        var response = await _httpClient.PostAsJsonAsync(
            baseApiUrl,
            new CreateFileImportRequest
            {
                FileName = s3Key,
                TotalRowsToProcess = recordCount
            },
            cancellationToken);

        response.EnsureSuccessStatusCode();

        var dto = await response.Content.ReadFromJsonAsync<FileImportStatusDto>(cancellationToken);
        if (dto != null)
        {
            return dto.Id;
        }
        throw new DomainException("Failed to create file import for " + s3Key + "");
    }
}