using CadsBridge.Infrastructure.Logging.Enrichers;
using Serilog;
using System.Diagnostics.CodeAnalysis;

namespace CadsBridge.Configuration;

public static class CdpLogging
{
    [ExcludeFromCodeCoverage]
    public static void Configuration(HostBuilderContext ctx, LoggerConfiguration config)
    {
        var traceIdHeader = ctx.Configuration.GetValue<string>("TraceHeader");
        var serviceVersion = Environment.GetEnvironmentVariable("SERVICE_VERSION") ?? "";

        config
            .ReadFrom.Configuration(ctx.Configuration)
            .Enrich.FromLogContext()
            .Enrich.With(new HttpContextEnricher(new HttpContextAccessor()))
            .Enrich.WithProperty("service.version", serviceVersion);

        if (!string.IsNullOrWhiteSpace(traceIdHeader))
        {
            config.Enrich.WithCorrelationId(traceIdHeader);
        }
    }
}