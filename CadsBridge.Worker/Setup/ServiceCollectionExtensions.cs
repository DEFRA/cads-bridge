using CadsBridge.Worker.Configuration;
using CadsBridge.Worker.Jobs;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Quartz;
using System.Diagnostics.CodeAnalysis;
using CadsBridge.Worker.Tasks;

namespace CadsBridge.Worker.Setup;

[ExcludeFromCodeCoverage]
public static class ServiceCollectionExtensions
{
    public static void AddBackgroundServiceScheduling(this IServiceCollection services, IConfiguration configuration)
    {
        services
            .AddQuartzJobs(configuration)
            .AddJobs()
            .AddTasks();
    }

    private static IServiceCollection AddQuartzJobs(this IServiceCollection services, IConfiguration configuration)
    {
        var scheduledJobsConfiguration =
            configuration.GetRequiredSection("Quartz:Jobs").Get<List<ScheduledJobConfiguration>>() ?? [];
        if (scheduledJobsConfiguration.Count > 0)
        {
            services.AddQuartz(q =>
            {
                foreach (var jobConfiguration in scheduledJobsConfiguration)
                {
                    switch (jobConfiguration.JobType)
                    {
                        case nameof(BulkScanJob):
                            q.AddQuartzJob<BulkScanJob>(jobConfiguration);
                            break;
                        case nameof(DeltaScanJob):
                            q.AddQuartzJob<DeltaScanJob>(jobConfiguration);
                            break;
                        default:
                            throw new ArgumentException($"Unknown job type: {jobConfiguration.JobType}");
                    }
                }
            });
        }

        return services;
    }

    private static void AddQuartzJob<T>(this IServiceCollectionQuartzConfigurator quartzConfigurator,
        ScheduledJobConfiguration jobConfiguration) where T : IJob
    {
        if (jobConfiguration.Enabled && jobConfiguration.JobType == nameof(T) && jobConfiguration?.CronSchedule != null)
        {
            quartzConfigurator.AddJob<T>(opts => opts.WithIdentity(jobConfiguration.JobType));
            quartzConfigurator.AddTrigger(opts => opts
                .ForJob(jobConfiguration.JobType)
                .StartAt(jobConfiguration.EnabledFrom)
                .EndAt(jobConfiguration.EnabledTo)
                .WithIdentity($"{jobConfiguration.JobType}-trigger")
                .WithCronSchedule(jobConfiguration.CronSchedule));
        }
    }

    private static IServiceCollection AddJobs(this IServiceCollection services)
    {
        services.AddScoped<BulkScanJob>();
        services.AddScoped<DeltaScanJob>();
        return services;
    }

    private static IServiceCollection AddTasks(this IServiceCollection services)
    {
        services.AddScoped<BulkScanTask>();
        return services;
    }
}