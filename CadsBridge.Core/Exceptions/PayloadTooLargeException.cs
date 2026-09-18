namespace CadsBridge.Core.Exceptions;

public sealed class PayloadTooLargeException : DomainException
{
    public override string Title => "Content payload is too large";
    public PayloadTooLargeException(string message) : base(message) { }
    public PayloadTooLargeException(string message, Exception innerException)
        : base(message, innerException) { }
}
