namespace CadsBridge.Core.Exceptions;

public class PublishFailedException : Exception
{
    public bool IsTransient { get; }
    public PublishFailedException(string message, bool isTransient, Exception inner) : base(message, inner)
    {
        IsTransient = isTransient;
    }
    public PublishFailedException(string message, bool isTransient) : base(message)
    {
        IsTransient = isTransient;
    }
}