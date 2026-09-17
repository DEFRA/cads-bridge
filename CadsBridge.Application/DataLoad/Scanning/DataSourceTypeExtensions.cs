using CadsBridge.Application.Extensions;
using CadsBridge.Core.Attributes;

namespace CadsBridge.Application.DataLoad.Scanning;

public static class DataSourceTypeExtensions
{
    public static bool TryResolveDestinationPrefix(string sourceKey, out string? destinationPrefix)
    {
        destinationPrefix = null;

        if (string.IsNullOrWhiteSpace(sourceKey))
        {
            return false;
        }

        foreach (var dataSourceType in Enum.GetValues<DataSourceType>())
        {
            var info = dataSourceType.GetAttribute<DataSourceTypeInfoAttribute>();
            if (info is null)
            {
                continue;
            }

            var sourcePrefix = info.Prefix.TrimEnd('/') + "/";
            if (sourceKey.StartsWith(sourcePrefix, StringComparison.OrdinalIgnoreCase))
            {
                destinationPrefix = info.DestinationPrefix;
                return true;
            }
        }

        return false;
    }

    public static bool TryResolveDataSourceType(string destinationPrefix, out DataSourceType dataSourceType)
    {
        dataSourceType = default;

        if (string.IsNullOrWhiteSpace(destinationPrefix))
        {
            return false;
        }

        var normalized = destinationPrefix.Trim('/');

        foreach (var candidate in Enum.GetValues<DataSourceType>())
        {
            var info = candidate.GetAttribute<DataSourceTypeInfoAttribute>();
            if (info is null)
            {
                continue;
            }

            if (normalized.Equals(info.DestinationPrefix.Trim('/'), StringComparison.OrdinalIgnoreCase))
            {
                dataSourceType = candidate;
                return true;
            }
        }

        return false;
    }
}