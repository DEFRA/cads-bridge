using CadsBridge.Endpoints.Testing.Models;
using FluentValidation;

namespace CadsBridge.Endpoints.Testing.Validators;

public sealed class CreateTestFileRequestValidator : AbstractValidator<CreateTestFileRequest>
{
    public CreateTestFileRequestValidator()
    {
        RuleFor(x => x.FileName)
            .NotEmpty()
            .WithMessage("FileName is required.");

        RuleFor(x => x.Content)
            .NotEmpty()
            .WithMessage("Content must not be empty.");
    }
}