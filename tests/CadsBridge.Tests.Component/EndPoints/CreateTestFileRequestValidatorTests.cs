using CadsBridge.Application.DataLoad.Scanning;
using CadsBridge.Endpoints.Testing.Models;
using CadsBridge.Endpoints.Testing.Validators;
using FluentAssertions;

namespace CadsBridge.Tests.Component.EndPoints;

public class CreateTestFileRequestValidatorTests
{
    private readonly CreateTestFileRequestValidator _sut = new();

    [Fact]
    public void Validate_ShouldSucceed_ForValidRequest()
    {
        var request = new CreateTestFileRequest("some-file.csv", "some content", DataSourceType.CtsBulk);

        var result = _sut.Validate(request);

        result.IsValid.Should().BeTrue();
    }

    [Fact]
    public void Validate_ShouldFail_WhenFileNameIsEmpty()
    {
        var request = new CreateTestFileRequest(string.Empty, "some content", DataSourceType.CtsBulk);

        var result = _sut.Validate(request);

        result.IsValid.Should().BeFalse();
        result.Errors.Should().Contain(e => e.PropertyName == "FileName");
    }

    [Fact]
    public void Validate_ShouldFail_WhenContentIsEmpty()
    {
        var request = new CreateTestFileRequest("some-file.csv", string.Empty, DataSourceType.CtsBulk);

        var result = _sut.Validate(request);

        result.IsValid.Should().BeFalse();
        result.Errors.Should().Contain(e => e.PropertyName == "Content");
    }
}
