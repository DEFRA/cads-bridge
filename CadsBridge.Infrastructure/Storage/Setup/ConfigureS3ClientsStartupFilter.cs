using CadsBridge.Core.Storage.Abstractions;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;

namespace CadsBridge.Infrastructure.Storage.Setup;

public class ConfigureS3ClientsStartupFilter(IEnumerable<IConfigureS3Clients> configurers) : IStartupFilter
{
    private readonly IEnumerable<IConfigureS3Clients> _configurers = configurers;

    public Action<IApplicationBuilder> Configure(Action<IApplicationBuilder> next)
    {
        return app =>
        {
            var sp = app.ApplicationServices;

            foreach (var configurer in _configurers)
            {
                configurer.Configure(sp);
            }

            next(app);
        };
    }
}