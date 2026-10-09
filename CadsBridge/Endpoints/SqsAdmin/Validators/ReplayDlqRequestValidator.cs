using CadsBridge.Endpoints.SqsAdmin.Requests;
using FluentValidation;

namespace CadsBridge.Endpoints.SqsAdmin.Validators;

public class ReplayDlqRequestValidator : AbstractValidator<ReplayDlqRequest>
{
    public ReplayDlqRequestValidator()
    {
        RuleFor(x => x.BatchSize)
            .InclusiveBetween(1, 10)
            .When(x => x.BatchSize.HasValue)
            .WithMessage("BatchSize must be between 1 and 10 when specified.");
    }
}