using CadsBridge.Middleware;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using System.Diagnostics.CodeAnalysis;

namespace CadsBridge.Setup;

public static class WebApplicationExtensions
{
    [ExcludeFromCodeCoverage]
    public static void ConfigureRequestPipeline(this WebApplication app)
    {
        var env = app.Services.GetRequiredService<IWebHostEnvironment>();
        var applicationLifetime = app.Services.GetRequiredService<IHostApplicationLifetime>();
        var logger = app.Services.GetRequiredService<ILogger<Program>>();
        var configuration = app.Services.GetRequiredService<IConfiguration>();
        var healthcheckMaskingEnabled = configuration.GetValue<bool>("HealthcheckMaskingEnabled");

        if (logger.IsEnabled(LogLevel.Information))
        {
            applicationLifetime.ApplicationStarted.Register(() =>
                logger.LogInformation("{ApplicationName} started", env.ApplicationName));
            applicationLifetime.ApplicationStopping.Register(() =>
                logger.LogInformation("{ApplicationName} stopping", env.ApplicationName));
            applicationLifetime.ApplicationStopped.Register(() =>
                logger.LogInformation("{ApplicationName} stopped", env.ApplicationName));
        }

        app.UseHeaderPropagation();
        app.UseRouting();

        app.UseMiddleware<ExceptionHandlingMiddleware>();

        app.UseAuthentication();
        app.UseAuthorization();

        app.MapControllers();

        app.MapGet("/", () => "Alive!").AllowAnonymous();

        app.MapHealthChecks("/health", new HealthCheckOptions()
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
        }).AllowAnonymous();
    }
}