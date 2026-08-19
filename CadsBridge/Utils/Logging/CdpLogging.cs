using Elastic.Serilog.Enrichers.Web;
using Serilog;
using System.Diagnostics.CodeAnalysis;

namespace CadsBridge.Utils.Logging;

public static class CdpLogging
{
    [ExcludeFromCodeCoverage]
    public static void Configuration(HostBuilderContext ctx, IHttpContextAccessor httpAccessor, LoggerConfiguration config)
    {
        var serviceVersion = Environment.GetEnvironmentVariable("SERVICE_VERSION") ?? "";

        config
            .ReadFrom.Configuration(ctx.Configuration)
            .Enrich.FromLogContext()
            .Enrich.WithEcsHttpContext(httpAccessor)
            .Enrich.With<Core.Correlation.CorrelationIdEnricher>()
            .Enrich.WithProperty("service.version", serviceVersion);
    }
}