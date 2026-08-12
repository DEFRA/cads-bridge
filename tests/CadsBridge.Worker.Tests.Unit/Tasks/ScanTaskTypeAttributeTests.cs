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
    }

    [Fact]
    public void GetDeltaTypeAttribute_ShouldReturnAttribute()
    {
        var testEnum = ScanTaskType.Delta;

        var result = testEnum.GetAttribute<ScanTaskInfoAttribute>();
        result.Should().NotBeNull();
        result!.Name.Should().Be("DELTA");
        result!.Prefix.Should().Be("cads/cts/daily");
    }
}