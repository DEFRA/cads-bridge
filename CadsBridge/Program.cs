using CadsBridge.Setup;
using CadsBridge.Utils.Http;
using CadsBridge.Utils.Logging;
using FluentValidation;
using Serilog;
using System.Diagnostics.CodeAnalysis;
using Amazon.SecurityToken;
using CadsBridge.Infrastructure.ApiClients.Configuration;
using CadsBridge.Infrastructure.Authentication.Fakes;
using CadsBridge.Infrastructure.Authentication.Providers;

var app = CreateWebApplication(args);
await app.RunAsync();
return;

[ExcludeFromCodeCoverage]
static WebApplication CreateWebApplication(string[] args)
{
    var builder = WebApplication.CreateBuilder(args);
    ConfigureBuilder(builder);

    var app = builder.Build();

    app.ConfigureRequestPipeline();

    return app;
}

[ExcludeFromCodeCoverage]
static void ConfigureBuilder(WebApplicationBuilder builder)
{
    builder.Configuration.AddEnvironmentVariables();

    // Give hosted background services time to gracefully finalise in-flight S3 transfers/uploads
    var shutdownTimeoutSeconds = builder.Configuration.GetValue("Host:ShutdownTimeoutSeconds", 25);
    builder.Services.Configure<HostOptions>(options =>
        options.ShutdownTimeout = TimeSpan.FromSeconds(shutdownTimeoutSeconds));

    // Configure logging to use the CDP Platform standards.
    builder.Services.AddHttpContextAccessor();
    builder.Host.UseSerilog((context, services, config) =>
        CdpLogging.Configuration(context, services.GetRequiredService<IHttpContextAccessor>(), config));

    var stsMockConfig = builder.Configuration.GetSection("ApiClients:CdsApi:Sts:Mock").Get<StsMockConfig>() ?? new StsMockConfig();
    if (stsMockConfig.Enabled)
    {
        builder.Services.AddHttpClient(MockSecurityTokenServiceClient.HttpClientName);
        builder.Services.AddSingleton<IAmazonSecurityTokenService>(sp => new MockSecurityTokenServiceClient(
            sp.GetRequiredService<IHttpClientFactory>(),
            stsMockConfig,
            sp.GetRequiredService<TimeProvider>()));
    }
    else
    {
        builder.Services.AddAWSService<IAmazonSecurityTokenService>();
    }
    builder.Services.AddSingleton<IAwsStsProvider, AwsStsProvider>();

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

    builder.Services.ConfigureCadsBridge(builder.Configuration);

    builder.Services.AddValidatorsFromAssemblyContaining<Program>();
}