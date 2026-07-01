using System.Net.Http.Json;
using CadsBridge.Infrastructure.ApiClients.Configuration;
using CadsBridge.Infrastructure.ApiClients.Contracts;
using CadsBridge.Infrastructure.ApiClients.DTOs;

namespace CadsBridge.Infrastructure.ApiClients.Services;


public class FileImportStatusApiService : IFileImportStatusApiService
{
    private readonly HttpClient _httpClient;

    public FileImportStatusApiService(IHttpClientFactory httpClientFactory)
    {
        _httpClient = httpClientFactory.CreateClient(nameof(ApiClientNames.CdsApi));
    }

    public async Task<FileImportStatusDto?> GetByS3Key(string s3Key, CancellationToken cancellationToken)
    {
        var response = await _httpClient.GetAsync(
            $"/api/get-endpoint?s3Key={s3Key}",
            cancellationToken);

        response.EnsureSuccessStatusCode();

        var result = await response.Content.ReadFromJsonAsync<FileImportStatusDto>(cancellationToken);
        return result;
    }

    public async Task<FileImportStatusDto?> GetById(Guid id, CancellationToken cancellationToken)
    {
        var response = await _httpClient.GetAsync(
            $"/api/get-endpoint?id={id}",
            cancellationToken);

        response.EnsureSuccessStatusCode();

        var result = await response.Content.ReadFromJsonAsync<FileImportStatusDto>(cancellationToken);
        return result;
    }

    public async Task<Guid> Create(string s3Key, long recordCount, CancellationToken cancellationToken)
    {
        var response = await _httpClient.PostAsJsonAsync(
            "/api/create-endpoint",
            new { s3Key, recordCount },
            cancellationToken);

        response.EnsureSuccessStatusCode();

        var result = await response.Content.ReadFromJsonAsync<Guid>(cancellationToken);
        return result;
    }
}