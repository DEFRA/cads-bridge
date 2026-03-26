using System.Diagnostics.CodeAnalysis;

namespace CadsBridge.Infrastructure.Configuration.Aws;

[ExcludeFromCodeCoverage]
public class AwsConfig
{
    public const string SectionName = "AWS";

    public string Region { get; set; } = string.Empty;
    public EmfConfig EMF { get; set; } = new();
    public MetricsConfig Metrics { get; set; } = new();
}

[ExcludeFromCodeCoverage]
public class EmfConfig
{
    public string Namespace { get; set; } = string.Empty;
    public string ServiceName { get; set; } = string.Empty;
}

[ExcludeFromCodeCoverage]
public class MetricsConfig
{
    public string MeterName { get; set; } = string.Empty;
}