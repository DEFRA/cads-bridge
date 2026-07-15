using CadsBridge.Infrastructure.ApiClients.Configuration;
using CadsBridge.Infrastructure.ApiClients.Contracts;
using CadsBridge.Infrastructure.ApiClients.Services;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;
using Polly;
using Polly.Retry;
using System.Net;

namespace CadsBridge.Infrastructure.ApiClients.Setup;

public static class ServiceCollectionExtensions
{
    public static void AddApiClients(
        this IServiceCollection services,
        IConfiguration configuration)
    {
        var apiClientsConfigs = configuration
            .GetSection("ApiClients")
            .Get<Dictionary<string, ApiClientConfiguration>>();

        if (apiClientsConfigs == null) return;

        services.AddSingleton(apiClientsConfigs);

        if (configuration.GetValue<bool>("ApiClients:CdsApi:UseFakeClient"))
        {
            services.AddTransient<IFileImportApiService, Fakes.FakeFileImportApiService>();
        }
        else
        {
            services.AddTransient<IFileImportApiService, FileImportApiService>();
        }

        var healthChecksBuilder = services.AddHealthChecks();

        foreach (var (clientName, clientConfig) in apiClientsConfigs)
        {
            // Skip if the BaseUrl is not a valid absolute URI
            if (!Uri.TryCreate(clientConfig.BaseUrl, UriKind.Absolute, out _))
            {
                continue;
            }

            // Skip if the client name is not a valid ApiClientNames enum value
            if (!Enum.TryParse<ApiClientNames>(clientName, out _))
            {
                continue;
            }

            services.RegisterNamedHttpClient(
                clientName,
                clientConfig);

            if (clientConfig.HealthcheckEnabled)
            {
                healthChecksBuilder.Add(new HealthCheckRegistration(
                    name: $"http-client-{clientName}",
                    factory: sp => new ApiClientHealthCheck(
                        sp.GetRequiredService<IHttpClientFactory>(),
                        clientName,
                        sp.GetRequiredService<ILogger<ApiClientHealthCheck>>()),
                    failureStatus: HealthStatus.Unhealthy,
                    tags: ["api-client"]
                ));
            }
        }
    }

    private static void RegisterNamedHttpClient(
        this IServiceCollection services,
        string clientName,
        ApiClientConfiguration clientConfig)
    {
        services.AddHttpClient(clientName, client =>
        {
            client.BaseAddress = new Uri(clientConfig.BaseUrl.TrimEnd('/'));
            if (clientConfig.BasicApiKey != null)
            {
                client.DefaultRequestHeaders.Add("Authorization", $"Basic {clientConfig.BasicApiKey}");
            }
            if (!string.IsNullOrWhiteSpace(clientConfig.XApiKey))
            {
                client.DefaultRequestHeaders.Add("x-api-key", clientConfig.XApiKey);
            }
        })
        .AddResilienceHandler(clientName, (builder, context) =>
        {
            var resilienceConfig = clientConfig.ResiliencePolicy;
            builder.AddRetry(new RetryStrategyOptions<HttpResponseMessage>
            {
                MaxRetryAttempts = resilienceConfig.Retries,
                Delay = TimeSpan.FromSeconds(resilienceConfig.BaseDelaySeconds),
                UseJitter = resilienceConfig.UseJitter,
                BackoffType = DelayBackoffType.Exponential,
                ShouldHandle = DefaultRetryPredicate
            });

            builder.AddTimeout(TimeSpan.FromSeconds(resilienceConfig.TimeoutPeriodSeconds));
        });
    }

    private static ValueTask<bool> DefaultRetryPredicate(RetryPredicateArguments<HttpResponseMessage> args)
    {
        var response = args.Outcome.Result;
        if (response != null &&
            response.StatusCode is
                >= HttpStatusCode.InternalServerError // This includes all 5xx status codes such as bad gateway, service unavailable, etc.
                or HttpStatusCode.RequestTimeout)
        {
            return ValueTask.FromResult(true);
        }

        if (args.Outcome.Exception is HttpRequestException)
        {
            return ValueTask.FromResult(true);
        }

        return ValueTask.FromResult(false);
    }
}