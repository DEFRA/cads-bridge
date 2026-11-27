using CadsBridge.Endpoints.Testing.Models;
using CadsBridge.Endpoints.Testing.Validators;
using FluentValidation.TestHelper;

namespace CadsBridge.Tests.Unit.Endpoints.Testing.Validators;

public class GetInternalFilesRequestValidatorTests
{
    private GetInternalFilesRequestValidator _validator;

    public GetInternalFilesRequestValidatorTests()
    {
        _validator = new GetInternalFilesRequestValidator();
    }

    [Fact]
    public async Task ValidateAsync_Should_Return_No_Errors()
    {
        var validFilename = "CTSM_CADS_ETE_BULK_00001_001_CT_LOCATIONS_2026-07-28-094638.csv";

        var request = new GetInternalFilesRequest(validFilename);

        var validationResult = await  _validator.ValidateAsync(request, TestContext.Current.CancellationToken);

        Assert.True(validationResult.IsValid);
    }

    [Fact]
    public async Task ValidateAsync_Should_Return_Error_When_File_Is_Not_ETE()
    {
        var validFilename = "CTSM_CADS_BULK_00001_001_CT_LOCATIONS_2026-07-28-094638.csv";

        var request = new GetInternalFilesRequest(validFilename);

        var validationResult = await  _validator.TestValidateAsync(request, null, TestContext.Current.CancellationToken);

        Assert.False(validationResult.IsValid);
        validationResult.ShouldHaveValidationErrorFor(x => x.FileName)
            .WithErrorMessage("Must be a valid ETE CTSM filename");
    }
}
