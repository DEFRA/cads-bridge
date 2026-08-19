using CadsBridge.Core.Domain.BusinessRules;
using System.Net;

namespace CadsBridge.Core.Exceptions;

public class BusinessRuleValidationException(IBusinessRule brokenRule) : Exception(brokenRule.Message)
{
    private IBusinessRule BrokenRule { get; } = brokenRule;

    public string Details { get; } = brokenRule.Message;

    public HttpStatusCode HttpStatusCode { get; } = brokenRule.HttpStatusCode;

    public override string ToString()
    {
        return $"{BrokenRule.GetType().FullName}: {BrokenRule.Message}";
    }
}