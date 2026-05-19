namespace CadsBridge.Core.DataSeed.Abstractions;

public interface IDataSeedFileLoader
{
    List<DataSeedFileDetail> GetFiles();
}

public record DataSeedFileDetail(string FileName, string FilePath);