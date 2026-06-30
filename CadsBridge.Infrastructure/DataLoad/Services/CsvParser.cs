using CadsBridge.Application.DataLoad.Services;
using CadsBridge.Core.Exceptions;

namespace CadsBridge.Infrastructure.DataLoad.Services;

public class CsvParser : ICsvParser
{
    public IEnumerable<string> ParseCsvLine(string csvLineContent, char delimiter = '|', int expectedCount = 0)
    {
        var parts = csvLineContent.Split('|');
        if (expectedCount > 0 && parts.Length != expectedCount)
        {
            throw new DomainException($"Trailer line has {parts.Length} field(s); expected 4. Line: '{csvLineContent}'");
        }
        return parts;
    }
}