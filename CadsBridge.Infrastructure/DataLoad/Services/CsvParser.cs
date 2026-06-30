using CadsBridge.Application.DataLoad.Services;
using CadsBridge.Core.Exceptions;

namespace CadsBridge.Infrastructure.DataLoad.Services;

public class CsvParser : ICsvParser
{
    public IEnumerable<string> ParseCsvLine(string csvContent, char delimiter = '|', int expectedCount = 0)
    {
        var parts = csvContent.Split('|');
        if (expectedCount > 0 && parts.Length != expectedCount)
        {
            throw new DomainException($"Trailer line has {parts.Length} field(s); expected 4. Line: '{csvContent}'");
        }
        return parts;
    }
}