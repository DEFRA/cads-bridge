using CadsBridge.Worker.Configuration;
using CadsBridge.Worker.Jobs;
using CadsBridge.Worker.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Quartz;
using System.Diagnostics.CodeAnalysis;

namespace CadsBridge.Worker.Setup;

[ExcludeFromCodeCoverage]
public static class ServiceCollectionExtensions
{
    private static readonly TimeZoneInfo SchedulerTimeZone = TimeZoneInfo.Utc;

    public static void AddBackgroundServiceScheduling(this IServiceCollection services, IConfiguration configuration)
    {
        services.AddQuartzJobs(configuration);
        services.AddJobs();
        services.AddTasks();
    }

    private static void AddQuartzJobs(this IServiceCollection services, IConfiguration configuration)
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

            services.AddQuartzHostedService(options =>
            {
                options.WaitForJobsToComplete = true;
            });
        }
    }

    private static void AddQuartzJob<T>(this IServiceCollectionQuartzConfigurator quartzConfigurator,
        ScheduledJobConfiguration jobConfiguration) where T : IJob
    {
        if (!jobConfiguration.Enabled)
        {
            return;
        }

        ValidateJobConfiguration(jobConfiguration);

        var startAtUtc = DateTime.SpecifyKind(jobConfiguration.EnabledFrom, DateTimeKind.Utc);
        var endAtUtc = DateTime.SpecifyKind(jobConfiguration.EnabledTo, DateTimeKind.Utc);

        quartzConfigurator.AddJob<T>(opts => opts.WithIdentity(jobConfiguration.JobType));
        quartzConfigurator.AddTrigger(opts => opts
            .ForJob(jobConfiguration.JobType)
            .StartAt(startAtUtc)
            .EndAt(endAtUtc)
            .WithIdentity($"{jobConfiguration.JobType}-trigger")
            .WithCronSchedule(jobConfiguration.CronSchedule, x => x
                .InTimeZone(SchedulerTimeZone)
                // StartAt is intentionally allowed to be in the past (EnabledFrom). DoNothing stops
                // Quartz treating the skipped occurrences as misfires and firing immediately on startup.
                .WithMisfireHandlingInstructionDoNothing()));
    }

    private static void ValidateJobConfiguration(ScheduledJobConfiguration jobConfiguration)
    {
        if (string.IsNullOrWhiteSpace(jobConfiguration.CronSchedule))
        {
            throw new InvalidOperationException(
                $"Job '{jobConfiguration.JobType}' is enabled but has no CronSchedule configured.");
        }

        if (!CronExpression.IsValidExpression(jobConfiguration.CronSchedule))
        {
            throw new InvalidOperationException(
                $"Job '{jobConfiguration.JobType}' has an invalid CronSchedule '{jobConfiguration.CronSchedule}'.");
        }

        if (jobConfiguration.EnabledFrom >= jobConfiguration.EnabledTo)
        {
            throw new InvalidOperationException(
                $"Job '{jobConfiguration.JobType}' has EnabledFrom ({jobConfiguration.EnabledFrom:o}) " +
                $"that is not before EnabledTo ({jobConfiguration.EnabledTo:o}).");
        }
    }

    private static void AddJobs(this IServiceCollection services)
    {
        services.AddScoped<BulkScanJob>();
        services.AddScoped<DeltaScanJob>();
    }

    private static void AddTasks(this IServiceCollection services)
    {
        services.AddScoped<IBulkFileScanTask, BulkFileScanTask>();
        services.AddScoped<IDeltaFileScanTask, DeltaFileScanTask>();
    }
}