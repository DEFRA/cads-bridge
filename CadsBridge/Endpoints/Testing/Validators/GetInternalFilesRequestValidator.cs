using CadsBridge.Infrastructure.DataLoad.Sources.Parsers;
using FluentValidation;

namespace CadsBridge.Endpoints.Testing.Validators;

public class GetInternalFilesRequestValidator : AbstractValidator<string?>
{
    private const string _propertyName = "fileName";

    public GetInternalFilesRequestValidator()
    {
        RuleFor(x=>x)
            .Must(x=> CtsmFilenameParser.TryParse(x, out CtsmFilename _))
            .WithName(_propertyName)
            .WithMessage("Must be a valid CTSM filename");
    }
}