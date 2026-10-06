using System.Diagnostics.CodeAnalysis;
using System.Net;

namespace CadsBridge.Utils.Http;

public class ProxyHttpMessageHandler : HttpClientHandler
{
    [ExcludeFromCodeCoverage]
    public ProxyHttpMessageHandler(ILogger<ProxyHttpMessageHandler>? logger = null)
    {
        var proxyUri = Environment.GetEnvironmentVariable("HTTP_PROXY");
        var proxy = new WebProxy { BypassProxyOnLocal = true };
        if (proxyUri != null)
        {
            if (logger != null && logger.IsEnabled(LogLevel.Debug))
            {
                logger.LogDebug("Creating proxy http client");
            }
            var uri = new UriBuilder(proxyUri).Uri;
            proxy.Address = uri;
        }
        else
        {
            if (logger != null && logger.IsEnabled(LogLevel.Debug))
            {
                logger.LogWarning("HTTP_PROXY is NOT set, proxy client will be disabled");
            }
        }

        Proxy = proxy;
        UseProxy = proxyUri != null;
    }
}