using CadsBridge.Core.DataLoad.Seeds;

namespace CadsBridge.Application.DataLoad.Services;

public interface IDataSeedFileLoadService
{
    List<DataSeedFileDetail> GetFiles();
}