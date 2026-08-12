using CadsBridge.Application.Extensions;
using FluentAssertions;
using System.ComponentModel;

namespace CadsBridge.Infrastructure.Tests.Unit.Extensions;

public class EnumAttributeExtensionsTests
{
    private enum TestEnum
    {
        [Description("TestMessage")]
        WithAttribute = 1,

        WithoutAttribute = 2
    }

    [Fact]
    public void GetAttribute_ShouldReturnAttribute_WhenPresent()
    {
        var result = TestEnum.WithAttribute.GetAttribute<DescriptionAttribute>();
        result.Should().NotBeNull();
        result!.Description.Should().Be("TestMessage");
    }

    [Fact]
    public void GetAttribute_ShouldReturnNull_WhenAttributeMissing()
    {
        var result = TestEnum.WithoutAttribute.GetAttribute<DescriptionAttribute>();
        result.Should().BeNull();
    }

    [Fact]
    public void GetAttribute_ShouldReturnNull_WhenEnumValueUndefined()
    {
        var undefined = (TestEnum)999;
        var result = undefined.GetAttribute<DescriptionAttribute>();
        result.Should().BeNull();
    }

    [Fact]
    public void GetAttribute_ShouldThrowArgumentNullException_WhenValueIsNull()
    {
        Enum? nullable = null;
        Action act = () => nullable!.GetAttribute<DescriptionAttribute>();
        act.Should().Throw<ArgumentNullException>();
    }
}