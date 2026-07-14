using CadsBridge.Endpoints;
using CadsBridge.Setup;
using CadsBridge.Utils.Http;
using CadsBridge.Utils.Logging;
using FluentValidation;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Serilog;
using System.Diagnostics.CodeAnalysis;

var app = CreateWebApplication(args);
await app.RunAsync();

return;

[ExcludeFromCodeCoverage]
static WebApplication CreateWebApplication(string[] args)
{
    var builder = WebApplication.CreateBuilder(args);
    ConfigureBuilder(builder);

    var app = builder.Build();
    return SetupApplication(app);
}

[ExcludeFromCodeCoverage]
static void ConfigureBuilder(WebApplicationBuilder builder)
{
    builder.Configuration.AddEnvironmentVariables();

    // Configure logging to use the CDP Platform standards.
    builder.Services.AddHttpContextAccessor();
    builder.Host.UseSerilog(CdpLogging.Configuration);

    // Default HTTP Client
    builder.Services
        .AddHttpClient("DefaultClient")
        .AddHeaderPropagation();

    // Proxy HTTP Client
    builder.Services.AddTransient<ProxyHttpMessageHandler>();
    builder.Services
        .AddHttpClient("proxy")
        .ConfigurePrimaryHttpMessageHandler<ProxyHttpMessageHandler>();

    // Propagate trace header.
    builder.Services.AddHeaderPropagation(options =>
    {
        var traceHeader = builder.Configuration.GetValue<string>("TraceHeader");
        if (!string.IsNullOrWhiteSpace(traceHeader))
        {
            options.Headers.Add(traceHeader);
        }
    });

    builder.Services.ConfigureCds(builder.Configuration);
    builder.Services.AddValidatorsFromAssemblyContaining<Program>();
}

[ExcludeFromCodeCoverage]
static WebApplication SetupApplication(WebApplication app)
{
    app.UseHeaderPropagation();
    app.UseRouting();

    var configuration = app.Services.GetRequiredService<IConfiguration>();
    var healthcheckMaskingEnabled = configuration.GetValue<bool>("HealthcheckMaskingEnabled");
    app.MapHealthChecks("/health", new HealthCheckOptions
    {
        Predicate = _ => true,
        ResponseWriter = (context, healthReport) =>
        {
            context.Response.ContentType = "application/json; charset=utf-8";
            return context.Response.WriteAsync(HealthCheckWriter.WriteHealthStatusAsJson(
                healthReport,
                healthcheckMaskingEnabled: healthcheckMaskingEnabled,
                excludeHealthy: false,
                indented: true));
        },
        ResultStatusCodes =
        {
            [HealthStatus.Healthy] = StatusCodes.Status200OK,
            [HealthStatus.Degraded] = StatusCodes.Status200OK,
            [HealthStatus.Unhealthy] = StatusCodes.Status503ServiceUnavailable
        }
    });

    app.CreateEndpoints();

    return app;
}