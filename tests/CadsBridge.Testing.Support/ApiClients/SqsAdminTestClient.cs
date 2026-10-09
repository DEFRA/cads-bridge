using System.Net.Http.Json;
using CadsBridge.Endpoints.SqsAdmin.Requests;
using CadsBridge.Endpoints.SqsAdmin.Responses;
using CadsBridge.Infrastructure.Json;
using CadsBridge.Testing.Support.Constants;
using Microsoft.AspNetCore.Mvc;

namespace CadsBridge.Testing.Support.ApiClients;

public static class SqsAdminTestClient
{
    public static async Task<HttpResponseMessage> GetQueuesAsync(
        HttpClient client,
        CancellationToken cancellationToken)
    {
        return await client.GetAsync(TestEndpointConstants.SqsAdminGetQueuesEndpoint, cancellationToken);
    }

    public static async Task<HttpResponseMessage> GetMetricsAsync(
        HttpClient client,
        string queue,
        CancellationToken cancellationToken)
    {
        var endpoint = string.Format(TestEndpointConstants.SqsAdminGetMetricsEndpoint, queue);
        return await client.GetAsync(endpoint, cancellationToken);
    }

    public static async Task<HttpResponseMessage> GetDlqMetricsAsync(
        HttpClient client,
        string queue,
        CancellationToken cancellationToken)
    {
        var endpoint = string.Format(TestEndpointConstants.SqsAdminGetDlqMetricsEndpoint, queue);
        return await client.GetAsync(endpoint, cancellationToken);
    }

    public static async Task<HttpResponseMessage> GetMessagesAsync(
        HttpClient client,
        string queue,
        int? maxMessages,
        CancellationToken cancellationToken)
    {
        var endpoint = string.Format(TestEndpointConstants.SqsAdminGetMessagesEndpoint, queue);

        if (maxMessages.HasValue)
        {
            endpoint += $"?maxMessages={maxMessages.Value}";
        }

        return await client.GetAsync(endpoint, cancellationToken);
    }

    public static async Task<HttpResponseMessage> GetDlqMessagesAsync(
        HttpClient client,
        string queue,
        int? maxMessages,
        CancellationToken cancellationToken)
    {
        var endpoint = string.Format(TestEndpointConstants.SqsAdminGetDlqMessagesEndpoint, queue);

        if (maxMessages.HasValue)
        {
            endpoint += $"?maxMessages={maxMessages.Value}";
        }

        return await client.GetAsync(endpoint, cancellationToken);
    }

    public static async Task<HttpResponseMessage> ReplayDlqAsync(
        HttpClient client,
        string queue,
        int? batchSize,
        CancellationToken cancellationToken)
    {
        var endpoint = string.Format(TestEndpointConstants.SqsAdminReplayDlqEndpoint, queue);

        var request = new ReplayDlqRequest(batchSize);

        return await client.PostAsJsonAsync(
            endpoint,
            request,
            JsonDefaults.DefaultOptionsWithStringEnumConversion,
            cancellationToken);
    }

    public static async Task<GetQueuesResponse?> ReadQueuesDtoAsync(
        HttpResponseMessage response,
        CancellationToken cancellationToken)
    {
        return await response.Content.ReadFromJsonAsync<GetQueuesResponse>(
            JsonDefaults.DefaultOptionsWithStringEnumConversion,
            cancellationToken);
    }

    public static async Task<GetQueueMetricsResponse?> ReadMetricsDtoAsync(
        HttpResponseMessage response,
        CancellationToken cancellationToken)
    {
        return await response.Content.ReadFromJsonAsync<GetQueueMetricsResponse>(
            JsonDefaults.DefaultOptionsWithStringEnumConversion,
            cancellationToken);
    }

    public static async Task<GetQueueMessagesResponse?> ReadMessagesDtoAsync(
        HttpResponseMessage response,
        CancellationToken cancellationToken)
    {
        return await response.Content.ReadFromJsonAsync<GetQueueMessagesResponse>(
            JsonDefaults.DefaultOptionsWithStringEnumConversion,
            cancellationToken);
    }

    public static async Task<ReplayDlqResponse?> ReadReplayDtoAsync(
        HttpResponseMessage response,
        CancellationToken cancellationToken)
    {
        return await response.Content.ReadFromJsonAsync<ReplayDlqResponse>(
            JsonDefaults.DefaultOptionsWithStringEnumConversion,
            cancellationToken);
    }

    public static async Task<ProblemDetails?> ReadProblemDetailsAsync(
        HttpResponseMessage response,
        CancellationToken cancellationToken)
    {
        return await response.Content.ReadFromJsonAsync<ProblemDetails>(
            JsonDefaults.DefaultOptionsWithStringEnumConversion,
            cancellationToken);
    }
}