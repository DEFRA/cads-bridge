using CadsBridge.Core.DataSeed.Abstractions;

namespace CadsBridge.Infrastructure.DataSeed;

public class DataSeedFileLoader : IDataSeedFileLoader
{
    private const string SqlRootDirectory = "sql";

    public List<DataSeedFileDetail> GetFiles()
    {
        var sqlRoot = Path.Combine(AppContext.BaseDirectory, SqlRootDirectory);

        if (!Directory.Exists(sqlRoot))
            return [];

        var latestSubDirectory = Directory
            .GetDirectories(sqlRoot)
            .Select(Path.GetFileName)
            .Where(name => !string.IsNullOrEmpty(name))
            .Order(StringComparer.OrdinalIgnoreCase)
            .LastOrDefault();

        if (latestSubDirectory is null)
            return [];

        return Directory
            .GetFiles(Path.Combine(sqlRoot, latestSubDirectory), "*.sql")
            .Select(filePath =>
            {
                var fileName = Path.GetFileName(filePath);
                var relativePath = $"{SqlRootDirectory}/{latestSubDirectory}/{fileName}";
                return new DataSeedFileDetail(fileName, relativePath);
            })
            .OrderBy(f => f.FileName, StringComparer.OrdinalIgnoreCase)
            .ToList();
    }
}