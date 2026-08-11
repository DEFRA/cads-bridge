using System.ComponentModel;
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

    public static IEnumerable<T>? GetAttributes<T>(this Enum value)
        where T : Attribute
    {
        ArgumentNullException.ThrowIfNull(value);

        var type = value.GetType();
        var name = Enum.GetName(type, value);
        if (name == null) return null;

        var field = type.GetField(name);

        // Get all attributes
        var attributes = Attribute.GetCustomAttributes(field!, typeof(T));

        return (T[])attributes;
    }

    public static string GetDescription(this Enum value)
    {
        var descriptionAttribute = value.GetAttribute<DescriptionAttribute>();

        return descriptionAttribute != null ? descriptionAttribute.Description : string.Empty;
    }
}