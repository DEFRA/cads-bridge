namespace CadsBridge.Core.Exceptions;

public class DomainException : Exception
{
    public virtual string Title => "Bad Request";
    public DomainException(string message) : base(message) { }
    public DomainException(string message, Exception innerException)
        : base(message, innerException) { }
}