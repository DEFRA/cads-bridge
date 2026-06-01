namespace CadsBridge.Core.Exceptions;

public class RetriesExceededException(string message) : Exception(message)
{ }