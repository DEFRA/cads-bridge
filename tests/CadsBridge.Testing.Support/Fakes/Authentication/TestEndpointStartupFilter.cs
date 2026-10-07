using CadsBridge.Infrastructure.Authentication.Configuration;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;

namespace CadsBridge.Testing.Support.Fakes.Authentication;

public class TestEndpointStartupFilter : IStartupFilter
{
    public Action<IApplicationBuilder> Configure(Action<IApplicationBuilder> next)
    {
        return app =>
        {
            next(app);

            app.UseEndpoints(endpoints =>
            {
                var group = endpoints.MapGroup("/test-auth");

                group.MapGet("/azuread/sqs-admin", () => "OK: AzureAD SqsAdminManager")
                    .RequireAuthorization(AuthenticationConstants.AadSqsAdminExecutePolicy);
            });
        };
    }
}