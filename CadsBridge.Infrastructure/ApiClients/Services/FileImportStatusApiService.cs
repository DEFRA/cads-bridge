using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using CadsBridge.Core.Exceptions;
using CadsBridge.Infrastructure.ApiClients.Configuration;
using CadsBridge.Infrastructure.ApiClients.Contracts;
using CadsBridge.Infrastructure.ApiClients.DTOs;
using CadsBridge.Infrastructure.ApiClients.DTOs.Requests;
using Microsoft.Extensions.Logging;

namespace CadsBridge.Infrastructure.ApiClients.Services;

public class FileImportStatusApiService(IHttpClientFactory httpClientFactory, ILogger<FileImportStatusApiService> logger) : IFileImportStatusApiService
{
    private readonly HttpClient _httpClient = httpClientFactory.CreateClient(nameof(ApiClientNames.CdsApi));
    private const string baseApiUrl = "api/v1/systemadmin/fileimports";
    private const string getByFileNameEndpoint = "search";
    private const string markReset = "reset";

    private static readonly IReadOnlyDictionary<FileImportStatus, string> FileImportStatusUrlMap =
        new Dictionary<FileImportStatus, string>
        {
            { FileImportStatus.Importing, "importing" },
            { FileImportStatus.Completed, "complete" },
            { FileImportStatus.Failed, "failed" }
        };

    public async Task<FileImportStatusDto?> GetByFileName(string fileName, CancellationToken cancellationToken)
    {
        var endpoint = $"{baseApiUrl}/{getByFileNameEndpoint}?fileName={fileName}";
        var context = $"Getting file import status for '{fileName}'";
        return await GetRequestToApiAsync<FileImportStatusDto>(endpoint, context, cancellationToken);
    }

    public async Task<long> Create(string s3Key, long recordCount, CancellationToken cancellationToken)
    {
        var context = $"Creating file import for '{s3Key}' with {recordCount} records";
        var body = new CreateFileImportRequest
        {
            FileName = s3Key,
            TotalRowsToProcess = recordCount
        };

        var response = await PostRequestToApiAsync(baseApiUrl, body, context, cancellationToken);

        var dto = await ReadJsonOrThrowAsync<FileImportStatusDto>(response, context, cancellationToken);
        return dto.Id;
    }

    public async Task MarkStatus(long id, FileImportStatus status, CancellationToken cancellationToken)
    {
        if (!FileImportStatusUrlMap.TryGetValue(status, out var statusSegment))
        {
            throw new DomainException($"No URL mapping exists for file import status '{status}'.");
        }

        var endPoint = $"{baseApiUrl}/{id}/{statusSegment}";
        var context = $"Marking status of file import with id {id} as {status}";
        await PostRequestToApiAsync<object?>(endPoint, null, context, cancellationToken);
    }

    public async Task MarkReset(long id, CancellationToken cancellationToken)
    {
        var endPoint = $"{baseApiUrl}/{id}/{markReset}";
        var context = $"Resetting file import with id {id}";
        await PostRequestToApiAsync<object?>(endPoint, null, context, cancellationToken);
    }

    private async Task<T> GetRequestToApiAsync<T>(string requestUri, string context, CancellationToken cancellationToken)
    {
        var response = await SendAsync(ct => _httpClient.GetAsync(requestUri, ct), context, cancellationToken);
        return await ReadJsonOrThrowAsync<T>(response, context, cancellationToken);
    }

    private async Task<HttpResponseMessage> PostRequestToApiAsync<T>(string requestUri, T body, string context, CancellationToken cancellationToken)
    {
        var response = await SendAsync(ct => _httpClient.PostAsJsonAsync(requestUri, body, ct), context, cancellationToken);
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
        if (logger.IsEnabled(LogLevel.Debug))
        {
            logger.LogDebug("Initiating API call: {Context}", context);
        }

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
            var result = await response.Content.ReadFromJsonAsync<T>(cancellationToken);
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

        throw new NonRetryableException(
            $"Permanent failure when calling {context}. " +
            $"Status: {(int)response.StatusCode} {response.ReasonPhrase}. Response: {content}");
    }
}