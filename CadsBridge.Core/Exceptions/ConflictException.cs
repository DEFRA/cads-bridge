namespace CadsBridge.Core.Exceptions;

public sealed class ConflictException : DomainException
{
    public override string Title => "Conflict";
    public ConflictException(string message) : base(message) { }
    public ConflictException(string message, Exception innerException)
        : base(message, innerException) { }
}

