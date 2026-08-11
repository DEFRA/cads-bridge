using System;
using FluentAssertions;
using CadsBridge.Application.Extensions;
using Xunit;

namespace CadsBridge.Infrastructure.Tests.Unit.Extensions;

public class EnumAttributeExtensionsTests
{
    private enum TestEnum
    {
        [Obsolete("TestMessage")]
        WithAttribute = 1,

        WithoutAttribute = 2
    }

    [Fact]
    public void GetAttribute_ShouldReturnAttribute_WhenPresent()
    {
        var result = TestEnum.GetAttribute<ObsoleteAttribute>();
        result.Should().NotBeNull();
        result!.Message.Should().Be("TestMessage");
    }

    [Fact]
    public void GetAttribute_ShouldReturnNull_WhenAttributeMissing()
    {
        var result = TestEnum.GetAttribute<ObsoleteAttribute>();
        result.Should().BeNull();
    }

    [Fact]
    public void GetAttribute_ShouldReturnNull_WhenEnumValueUndefined()
    {
        var undefined = (TestEnum)999;
        var result = undefined.GetAttribute<ObsoleteAttribute>();
        result.Should().BeNull();
    }

    [Fact]
    public void GetAttribute_ShouldThrowArgumentNullException_WhenValueIsNull()
    {
        Enum? nullable = null;
        Action act = () => nullable!.GetAttribute<ObsoleteAttribute>();
        act.Should().Throw<ArgumentNullException>();
    }
}