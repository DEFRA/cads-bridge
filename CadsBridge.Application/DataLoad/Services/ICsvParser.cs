namespace CadsBridge.Application.DataLoad.Services;

public interface ICsvParser
{
    IEnumerable<string> ParseCsvLine(string csvContent, char delimiter = '|', int expectedCount = 0);
}