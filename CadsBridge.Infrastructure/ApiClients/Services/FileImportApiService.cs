using CadsBridge.Core.ApiClients;
using CadsBridge.Core.Exceptions;
using CadsBridge.Infrastructure.ApiClients.Configuration;
using CadsBridge.Infrastructure.ApiClients.Contracts;
using CadsBridge.Infrastructure.ApiClients.DTOs;
using CadsBridge.Infrastructure.ApiClients.DTOs.Requests;
using CadsBridge.Infrastructure.Json;
using Microsoft.Extensions.Logging;
using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace CadsBridge.Infrastructure.ApiClients.Services;

public class FileImportApiService(
    IHttpClientFactory httpClientFactory,
    ILogger<FileImportApiService> logger)
    : IFileImportApiService
{
    private readonly HttpClient _httpClient = httpClientFactory.CreateClient(nameof(ApiClientNames.CdsApi));
    private const string BaseApiUrl = "api/v1/systemadmin/fileimports";
    private const string GetByFileNameEndpoint = "search";
    private const string MarkResetEndpoint = "reset";
    private const string MarkFailedEndpoint = "failed";

    private static readonly Dictionary<FileImportStatus, string> s_fileImportStatusUrlMap =
        new()
        {
            { FileImportStatus.Transferred, "transferred" },
            { FileImportStatus.Split, "split" },
            { FileImportStatus.Completed, "completed" },
            { FileImportStatus.Failed, "failed" }
       };

    public async Task<FileImportDto?> GetByFileNameIfExists(string objectKey, CancellationToken cancellationToken)
    {
        var endpoint = $"{BaseApiUrl}/{GetByFileNameEndpoint}?fileName={Uri.EscapeDataString(objectKey)}";
        var context = $"Getting file import status for '{objectKey}' if it exists";
        if (logger.IsEnabled(LogLevel.Information))
        {
            logger.LogInformation("Initiating Get API call '{requestUri}': '{Context}'", endpoint, context);
        }

        try
        {
            var response = await SendAsync(ct => _httpClient.GetAsync(endpoint, ct), context, cancellationToken);

            if (logger.IsEnabled(LogLevel.Information))
            {
                logger.LogInformation("API call succeeded: {Context}", context);
            }

            return await ReadJsonOrThrowAsync<FileImportDto>(response, context, cancellationToken);
        }
        catch (NonRetryableException ex)
        {
            if (ex.Message.Contains("404 Not Found"))
            {
                return null;
            }
            throw;
        }

    }

    public async Task<FileImportDto?> GetByFileName(string objectKey, CancellationToken cancellationToken)
    {
        var endpoint = $"{BaseApiUrl}/{GetByFileNameEndpoint}?fileName={Uri.EscapeDataString(objectKey)}";
        var context = $"Getting file import status for '{objectKey}'";
        return await GetRequestToApiAsync<FileImportDto>(endpoint, context, cancellationToken);
    }

    public async Task<long> Create(string objectKey, long totalRowsToProcess, CancellationToken cancellationToken)
    {
        var context = $"Creating file import for '{objectKey}' with {totalRowsToProcess} records";
        var body = new CreateFileImportRequest
        {
            FileName = objectKey,
            TotalRowsToProcess = totalRowsToProcess
        };

        var response = await PostRequestToApiAsync(BaseApiUrl, body, context, cancellationToken);

        var dto = await ReadJsonOrThrowAsync<FileImportDto>(response, context, cancellationToken);

        if (dto.Id == 0)
        {
            // This is a temporary workaround for a bug in the API where it returns 0 for the ID on creation. We will attempt to retrieve the record by file name to get the correct ID.
            dto = await GetByFileName(objectKey, cancellationToken) ?? throw new NonRetryableException($"Failed to retrieve file import for '{objectKey}'.");
        }
        return dto.Id;
    }

    public async Task Update(long id, UpdateFileImportRequest request, CancellationToken cancellationToken)
    {
        var context = $"Updating file import for id: '{id}'";
        var endPoint = $"{BaseApiUrl}/{id}";

        await PutRequestToApiAsync(endPoint, request, context, cancellationToken);
    }

    public async Task MarkStatus(long id, FileImportStatus status, CancellationToken cancellationToken)
    {
        if (!s_fileImportStatusUrlMap.TryGetValue(status, out var statusSegment))
        {
            throw new DomainException($"No URL mapping exists for file import status '{status}'.");
        }

        var endPoint = $"{BaseApiUrl}/{id}/{statusSegment}";
        var context = $"Marking status of file import with id {id} as {status}";
        await PostRequestToApiAsync<object?>(endPoint, null, context, cancellationToken);
    }

    public async Task MarkFailed(long id, string reason, CancellationToken cancellationToken)
    {
        var endPoint = $"{BaseApiUrl}/{id}/{MarkFailedEndpoint}";
        var context = $"Marking file import with id {id} as failed";
        await PostRequestToApiAsync<object?>(endPoint, new { reason }, context, cancellationToken);
    }

    public async Task MarkReset(long id, CancellationToken cancellationToken)
    {
        var endPoint = $"{BaseApiUrl}/{id}/{MarkResetEndpoint}";
        var context = $"Resetting file import with id {id}";
        await PostRequestToApiAsync<object?>(endPoint, null, context, cancellationToken);
    }

    private async Task<T> GetRequestToApiAsync<T>(string requestUri, string context, CancellationToken cancellationToken)
    {
        if (logger.IsEnabled(LogLevel.Information))
        {
            logger.LogInformation("Initiating Get API call '{requestUri}': '{Context}'", requestUri, context);
        }

        var response = await SendAsync(ct => _httpClient.GetAsync(requestUri, ct), context, cancellationToken);

        if (logger.IsEnabled(LogLevel.Information))
        {
            logger.LogInformation("API call succeeded: {Context}", context);
        }

        return await ReadJsonOrThrowAsync<T>(response, context, cancellationToken);
    }

    private async Task<HttpResponseMessage> PostRequestToApiAsync<T>(string requestUri, T body, string context, CancellationToken cancellationToken)
    {
        if (logger.IsEnabled(LogLevel.Information))
        {
            logger.LogInformation("Initiating Post API call '{requestUri}': '{Context}'", requestUri, context);
        }

        var response = await SendAsync(ct => _httpClient.PostAsJsonAsync(requestUri, body, options: JsonDefaults.DefaultOptionsWithStringEnumConversion, ct), context, cancellationToken);

        if (logger.IsEnabled(LogLevel.Information))
        {
            logger.LogInformation("API call succeeded: {Context}", context);
        }

        return response;
    }

    private async Task<HttpResponseMessage> PutRequestToApiAsync<T>(string requestUri, T body, string context, CancellationToken cancellationToken)
    {
        if (logger.IsEnabled(LogLevel.Information))
        {
            logger.LogInformation("Initiating Put API call '{requestUri}': '{Context}'", requestUri, context);
        }

        var response = await SendAsync(ct => _httpClient.PutAsJsonAsync(requestUri, body, options: JsonDefaults.DefaultOptionsWithStringEnumConversion, ct), context, cancellationToken);

        if (logger.IsEnabled(LogLevel.Information))
        {
            logger.LogInformation("API call succeeded: {Context}", context);
        }

        return response;
    }

    // Single place that performs the HTTP call and maps transport-level faults to
    // Retryable/NonRetryable exceptions, so GET/POST callers stay free of duplicated try/catch.
    private async Task<HttpResponseMessage> SendAsync(
        Func<CancellationToken, Task<HttpResponseMessage>> send,
        string context,
        CancellationToken cancellationToken)
    {
        try
        {
            var response = await send(cancellationToken);

            if (!response.IsSuccessStatusCode)
            {
                await HandleFailedRequestAsync(response, context, cancellationToken);
            }

            return response;
        }
        catch (HttpRequestException ex)
        {
            if (logger.IsEnabled(LogLevel.Error))
            {
                logger.LogError(ex, "Network failure during API call: {Context}", context);
            }
            throw new RetryableException($"Network failure when calling {context}.", ex);
        }
        catch (TaskCanceledException ex) when (!cancellationToken.IsCancellationRequested)
        {
            if (logger.IsEnabled(LogLevel.Error))
            {
                logger.LogError(ex, "Timeout during API call: {Context}", context);
            }
            throw new RetryableException($"Timeout when calling {context}.", ex);
        }
    }

    private async Task<T> ReadJsonOrThrowAsync<T>(HttpResponseMessage response, string context, CancellationToken cancellationToken)
    {
        try
        {
            var options = new JsonSerializerOptions
            {
                PropertyNameCaseInsensitive = true,
                Converters = { new JsonStringEnumConverter() }
            };
            var result = await response.Content.ReadFromJsonAsync<T>(options, cancellationToken);
            if (result is null)
            {
                if (logger.IsEnabled(LogLevel.Error))
                {
                    logger.LogError("Deserialization returned null: {Context}", context);
                }
                throw new NonRetryableException($"Deserialization returned null for {context}.");
            }

            return result;
        }
        catch (JsonException ex)
        {
            if (logger.IsEnabled(LogLevel.Error))
            {
                logger.LogError(ex, "Deserialization error: {Context}", context);
            }
            throw new NonRetryableException($"Deserialization error for {context}.", ex);
        }
    }

    private async Task HandleFailedRequestAsync(HttpResponseMessage response, string context,
        CancellationToken cancellationToken)
    {
        var content = await response.Content.ReadAsStringAsync(cancellationToken);
        if (logger.IsEnabled(LogLevel.Warning))
        {
            logger.LogWarning("API call failed: {Context}, Status: {Status}, Response: {Response}",
                context, response.StatusCode, content);
        }
        if ((int)response.StatusCode >= 500 || response.StatusCode == HttpStatusCode.RequestTimeout)
        {
            throw new RetryableException(
                $"Transient failure when calling {context}. " +
                $"Status: {(int)response.StatusCode} {response.ReasonPhrase}. Response: {content}");
        }

        if (response.StatusCode == HttpStatusCode.Conflict)
        {
            throw new ConflictException(
                $"Conflict when calling {context}. " +
                $"Status: {(int)response.StatusCode} {response.ReasonPhrase}. Response: {content}");
        }

        throw new NonRetryableException(
            $"Permanent failure when calling {context}. " +
            $"Status: {(int)response.StatusCode} {response.ReasonPhrase}. Response: {content}");
    }
}