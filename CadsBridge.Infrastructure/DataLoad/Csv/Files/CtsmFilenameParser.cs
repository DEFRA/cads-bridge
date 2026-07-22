using System.Text.RegularExpressions;

namespace CadsBridge.Infrastructure.DataLoad.Csv.Files;

public static partial class CtsmFilenameParser
{
    // Pattern 1: CTSM_<app>_<env>_<type>_<batchId>_<partno>_<tablename>_<timestamp>.csv
    [GeneratedRegex(@"^CTSM_(?<app>[A-Za-z]+)_(?<env>[A-Za-z]+)_(?<type>[A-Za-z]+)_(?<batchId>[A-Za-z0-9#]+)_(?<partno>[A-Za-z0-9]+)_(?<tablename>[A-Za-z0-9_]+)_(?<timestamp>\d{4}-\d{2}-\d{2}-\d{6})", RegexOptions.Compiled | RegexOptions.CultureInvariant)]
    private static partial Regex Pattern1();

    // Pattern 2: CTSM_<app>_<env>_<type>_<batchId>_<tablename>_<timestamp>.csv
    [GeneratedRegex(@"^CTSM_(?<app>[A-Za-z]+)_(?<env>[A-Za-z]+)_(?<type>[A-Za-z]+)_(?<batchId>[A-Za-z0-9#]+)_(?<tablename>[A-Za-z0-9_]+)_(?<timestamp>\d{4}-\d{2}-\d{2}-\d{6})", RegexOptions.Compiled | RegexOptions.CultureInvariant)]
    private static partial Regex Pattern2();

    public static bool TryParse(string filename, out CtsmFilename? result)
    {
        result = null;

        if (string.IsNullOrWhiteSpace(filename))
            return false;

        // Try new pattern with part number (for CADS)
        var m1 = Pattern1().Match(filename);
        if (m1.Success)
        {
            result = new CtsmFilename(
                App: m1.Groups["app"].Value,
                Env: m1.Groups["env"].Value,
                Type: m1.Groups["type"].Value,
                BatchId: m1.Groups["batchId"].Value,
                PartNo: m1.Groups["partno"].Value,
                TableName: m1.Groups["tablename"].Value,
                Timestamp: m1.Groups["timestamp"].Value
            );
            return true;
        }

        // Try old pattern
        var m2 = Pattern2().Match(filename);
        if (m2.Success)
        {
            result = new CtsmFilename(
                App: m2.Groups["app"].Value,
                Env: m2.Groups["env"].Value,
                Type: m2.Groups["type"].Value,
                BatchId: m2.Groups["batchId"].Value,
                PartNo: null,
                TableName: m2.Groups["tablename"].Value,
                Timestamp: m2.Groups["timestamp"].Value
            );
            return true;
        }

        return false;
    }

    public static CtsmFilename? Parse(string filename)
    {
        if (!TryParse(filename, out var parsed))
        {
            throw new FormatException($"Invalid CTSM filename format: {filename}");
        }

        return parsed;
    }
}