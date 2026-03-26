namespace CadsBridge.Core.Exceptions;

public sealed class NotFoundException : DomainException
{
    public override string Title => "Not Found";
    public NotFoundException(string name, object key)
        : base($"'{name}' ({key}) was not found.") { }
    public NotFoundException(string message) : base(message) { }
    public NotFoundException(string message, Exception innerException)
        : base(message, innerException) { }
}