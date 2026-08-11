namespace CadsBridge.Core.Attributes;

[AttributeUsage(AttributeTargets.Field, Inherited = false, AllowMultiple = true)]
public sealed class ScanTaskInfoAttribute(string name, string prefix) : Attribute
{
    public string Name { get; } = name;

    public string Prefix { get; } = prefix;
}