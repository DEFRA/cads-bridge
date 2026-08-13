using CadsBridge.Core.ApiClients;
using CadsBridge.Core.Domain.BusinessRules;

namespace CadsBridge.Infrastructure.DataLoad;

public class DestinationTableNameIsValid(FileImport fileImport) : IBusinessRule
{
    public static readonly string UnknownDestinationTableName = "UNKNOWN";
    public bool IsBroken()
    {
        return fileImport is null || fileImport.DestinationTableName == UnknownDestinationTableName;
    }

    public string Message => $"Destination table could not be validated for {fileImport.FileName}";
}