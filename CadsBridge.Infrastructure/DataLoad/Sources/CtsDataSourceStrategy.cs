using CadsBridge.Application.DataLoad.Sources;
using CadsBridge.Infrastructure.DataLoad.Sources.Parsers;

namespace CadsBridge.Infrastructure.DataLoad.Sources;

public sealed class CtsDataSourceStrategy : IDataSourceStrategy
{
    public bool IsFileNameValidForType(string fileName, string expectedType) =>
        CtsmFilenameParser.TryParse(fileName, out var parsed) &&
        parsed!.Type.Equals(expectedType, StringComparison.OrdinalIgnoreCase);

    public string DeriveDecryptionPassword(string fileName) =>
        CtsmFilenameParser.Parse(fileName)!.DerivePassword();
}