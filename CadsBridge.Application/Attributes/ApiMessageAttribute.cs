namespace CadsBridge.Application.Attributes;

[AttributeUsage(AttributeTargets.Method)]
public class ApiMessageAttribute(string message, string? description = null) : Attribute
{
    public string Message { get; } = message;
    public string? Description { get; } = description;
}