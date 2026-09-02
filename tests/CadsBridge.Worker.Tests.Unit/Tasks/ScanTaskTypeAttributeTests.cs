using CadsBridge.Application.Extensions;
using CadsBridge.Core.Attributes;
using CadsBridge.Worker.Tasks;
using FluentAssertions;

namespace CadsBridge.Worker.Tests.Unit.Tasks;

public class ScanTaskTypeAttributeTests
{

    [Fact]
    public void GetBulkTypeAttribute_ShouldReturnAttribute()
    {
        var testEnum = ScanTaskType.Bulk;

        var result = testEnum.GetAttribute<ScanTaskInfoAttribute>();
        result.Should().NotBeNull();
        result!.Name.Should().Be("BULK");
        result!.Prefix.Should().Be("cads/cts/bulk");
        result!.DestinationPrefix.Should().Be("import/cts/bulk");
    }

    [Fact]
    public void GetDeltaTypeAttribute_ShouldReturnAttribute()
    {
        var testEnum = ScanTaskType.Delta;

        var result = testEnum.GetAttribute<ScanTaskInfoAttribute>();
        result.Should().NotBeNull();
        result!.Name.Should().Be("DELTA");
        result!.Prefix.Should().Be("cads/cts/daily");
        result!.DestinationPrefix.Should().Be("import/cts/daily");
    }

    [Fact]
    public void EveryScanTaskType_ShouldFollowTheOwnerDataSourceTypeLayout()
    {
        foreach (var scanTaskType in Enum.GetValues<ScanTaskType>())
        {
            var info = scanTaskType.GetAttribute<ScanTaskInfoAttribute>();

            info.Should().NotBeNull($"{scanTaskType} must declare a ScanTaskInfo attribute");
            info!.Prefix.Split('/').Should().HaveCount(3, "source prefix should be cads/{{data_source}}/{{type}}");
            info.DestinationPrefix.Split('/').Should().HaveCount(3, "destination prefix should be import/{{data_source}}/{{type}}");
            info.DestinationPrefix.Should().StartWith("import/");

            var dataSourceAndType = info.Prefix[(info.Prefix.IndexOf('/') + 1)..];
            info.DestinationPrefix.Should().EndWith(dataSourceAndType,
                "the {{data_source}}/{{type}} part of the destination should mirror the source");
        }
    }
}
