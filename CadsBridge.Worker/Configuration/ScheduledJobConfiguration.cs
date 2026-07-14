namespace CadsBridge.Worker.Configuration;

public class ScheduledJobConfiguration
{
    public string JobType { get; set; } = string.Empty;
    public bool Enabled { get; set; }
    public string CronSchedule { get; set; } = string.Empty;
    public DateTime EnabledFrom { get; set; }
    public DateTime EnabledTo { get; set; }
}