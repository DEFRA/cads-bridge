namespace CadsBridge.Core.Exceptions;

public class NonRetryableException : Exception
{
    public NonRetryableException(string message, Exception inner) : base(message, inner)
    {
    }
    public NonRetryableException(string message) : base(message)
    {
    }
}