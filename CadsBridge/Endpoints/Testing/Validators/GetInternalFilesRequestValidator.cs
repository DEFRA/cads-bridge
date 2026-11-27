using CadsBridge.Endpoints.Testing.Models;
using CadsBridge.Infrastructure.DataLoad.Services.Testing;
using CadsBridge.Infrastructure.DataLoad.Sources.Parsers;
using FluentValidation;

namespace CadsBridge.Endpoints.Testing.Validators;

public class GetInternalFilesRequestValidator : AbstractValidator<GetInternalFilesRequest>
{
    private const string FilenameFormatErrorMessage = "Must be a valid ETE CTSM filename";

    public GetInternalFilesRequestValidator()
    {
        RuleLevelCascadeMode = CascadeMode.Stop;

        RuleFor(x => x.FileName)
            .Cascade(CascadeMode.Stop)
            .NotNull()
            .Matches(TestingConstants.ValidE2EFilenamePattern)
            .WithMessage(FilenameFormatErrorMessage)
            .Must(x => CtsmFilenameParser.TryParse(x, out CtsmFilename _))
            .WithMessage(FilenameFormatErrorMessage);
    }
}