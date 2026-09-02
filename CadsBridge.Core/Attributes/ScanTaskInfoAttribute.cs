namespace CadsBridge.Core.Attributes;

[AttributeUsage(AttributeTargets.Field, Inherited = false, AllowMultiple = true)]
public sealed class ScanTaskInfoAttribute(string name, string prefix, string destinationPrefix) : Attribute
{
    public string Name { get; } = name;

    public string Prefix { get; } = prefix;

    public string DestinationPrefix { get; } = destinationPrefix;
}