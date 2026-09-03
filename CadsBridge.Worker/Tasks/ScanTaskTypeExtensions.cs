using CadsBridge.Application.Extensions;
using CadsBridge.Core.Attributes;

namespace CadsBridge.Worker.Tasks;

public static class ScanTaskTypeExtensions
{
    public static bool TryResolveDestinationPrefix(string sourceKey, out string? destinationPrefix)
    {
        destinationPrefix = null;

        if (string.IsNullOrWhiteSpace(sourceKey))
        {
            return false;
        }

        foreach (var scanTaskType in Enum.GetValues<ScanTaskType>())
        {
            var info = scanTaskType.GetAttribute<ScanTaskInfoAttribute>();
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
}