using System.Net;

namespace CadsBridge.Core.Domain.BusinessRules;

public interface IBusinessRule
{
    HttpStatusCode HttpStatusCode => HttpStatusCode.BadRequest;
    bool IsBroken();
    string Message { get; }
}