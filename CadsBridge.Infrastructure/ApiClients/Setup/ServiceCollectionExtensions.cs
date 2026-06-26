using System.Net;
using CadsBridge.Infrastructure.ApiClients.Configuration;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;
using Polly;
using Polly.Retry;

namespace CadsBridge.Infrastructure.ApiClients.Setup;

public static class ServiceCollectionExtensions
{
    public static void AddApiClients(
        this IServiceCollection services,
        IConfiguration configuration,
        IHealthChecksBuilder healthChecksBuilder)
    {
        var apiClientsConfigs = configuration
            .GetSection("ApiClients")
            .Get<Dictionary<string, ApiClientConfiguration>>();

        if (apiClientsConfigs == null) return;

        services.AddSingleton(apiClientsConfigs);

        foreach (var (clientName, clientConfig) in apiClientsConfigs)
        {
            // Skip if the BaseUrl is not a valid absolute URI
            if (!Uri.TryCreate(clientConfig.BaseUrl, UriKind.Absolute, out _))
            {
                continue;
            }

            services.RegisterNamedHttpClient(clientName, clientConfig);

            if (clientConfig.HealthcheckEnabled)
            {
                // Dedicated probe client: same BaseAddress, NO resilience/retry wrapper,
                // so health checks fail fast and report the true downstream status.
                var healthClientName = HealthClientName(clientName);
                services.AddHttpClient(healthClientName, client =>
                {
                    client.BaseAddress = new Uri(clientConfig.BaseUrl.TrimEnd('/'));
                });

                healthChecksBuilder.Add(new HealthCheckRegistration(
                    name: $"http-client-{clientName}",
                    factory: sp => new ApiClientHealthCheck(
                        sp.GetRequiredService<IHttpClientFactory>(),
                        healthClientName,
                        clientName,
                        sp.GetRequiredService<ILogger<ApiClientHealthCheck>>()),
                    failureStatus: null,
                    tags: ["api-client"]
                ));
            }
        }
    }

    public static string HealthClientName(string clientName) => $"{clientName}-health";
    private static void RegisterNamedHttpClient(this IServiceCollection services, string clientName,
        ApiClientConfiguration clientConfig)
    {
        services.AddHttpClient(clientName, client =>
        {
            client.BaseAddress = new Uri(clientConfig.BaseUrl.TrimEnd('/'));
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