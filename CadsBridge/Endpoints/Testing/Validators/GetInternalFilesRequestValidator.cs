using System.Text.RegularExpressions;
using CadsBridge.Endpoints.Testing.Models;
using CadsBridge.Infrastructure.DataLoad.Sources.Parsers;
using FluentValidation;

namespace CadsBridge.Endpoints.Testing.Validators;

public class GetInternalFilesRequestValidator : AbstractValidator<GetInternalFilesRequest>
{
    private static readonly Regex s_eTePattern = new("CTSM_(?<app>[a-zA-Z]+)_ETE_", RegexOptions.CultureInvariant);
    private const string FilenameFormatErrorMessage = "Must be a valid ETE CTSM filename";

    public GetInternalFilesRequestValidator()
    {
        RuleFor(x => x.FileName)
            .NotNull()
            .Matches(s_eTePattern)
            .WithMessage(FilenameFormatErrorMessage)
            .Must(x => CtsmFilenameParser.TryParse(x, out CtsmFilename _))
            .WithMessage(FilenameFormatErrorMessage);
    }
}