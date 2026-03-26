using Microsoft.AspNetCore.Http;
using Serilog.Core;
using Serilog.Events;

namespace CadsBridge.Infrastructure.Logging.Enrichers;

public class HttpContextEnricher(IHttpContextAccessor accessor) : ILogEventEnricher
{
    private readonly IHttpContextAccessor _accessor = accessor;

    public void Enrich(LogEvent logEvent, ILogEventPropertyFactory propertyFactory)
    {
        var ctx = _accessor.HttpContext;
        if (ctx == null) return;

        var userAgent = ctx.Request.Headers.UserAgent.ToString();
        var clientIp = ctx.Connection.RemoteIpAddress?.ToString();

        logEvent.AddPropertyIfAbsent(propertyFactory.CreateProperty("http.user_agent", userAgent));
        logEvent.AddPropertyIfAbsent(propertyFactory.CreateProperty("client.ip", clientIp));
    }
}