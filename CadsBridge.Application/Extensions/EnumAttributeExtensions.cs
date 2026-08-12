using System.Reflection;

namespace CadsBridge.Application.Extensions;

public static class EnumAttributeExtensions
{
    public static T? GetAttribute<T>(this Enum value)
        where T : Attribute
    {
        ArgumentNullException.ThrowIfNull(value);

        var type = value.GetType();
        var name = Enum.GetName(type, value);
        if (name == null) return null;

        var field = type.GetField(name);
        if (field == null) return null;

        return field.GetCustomAttribute<T>();
    }
}