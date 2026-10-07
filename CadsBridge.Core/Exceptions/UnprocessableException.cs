namespace CadsBridge.Core.Exceptions;

public class UnprocessableException : DomainException
{
    public override string Title => "Unprocessable content";
    public UnprocessableException(string name, object key)
        : base($"'{name}' ({key}) was uprocessable.") { }
    public UnprocessableException(string message) : base(message) { }
    public UnprocessableException(string message, Exception innerException)
        : base(message, innerException) { }
}