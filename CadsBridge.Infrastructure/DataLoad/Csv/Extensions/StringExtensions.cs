namespace CadsBridge.Infrastructure.DataLoad.Csv.Extensions;

public static class StringExtensions
{
    extension(string s)
    {
        public string FormatKey(string destination, int partNumber = 1)
        {
            var fileName = $"{Path.GetFileNameWithoutExtension(s)}-part-{partNumber:D4}.csv";
            return string.IsNullOrEmpty(destination) ? fileName : $"{destination.TrimEnd('/')}/{fileName}";
        }
        public string FormatSplitFileTargetKey(int partNumber = 1)
        {
            var fileNameWithoutExtension = Path.GetFileNameWithoutExtension(s);
            return $"import/{fileNameWithoutExtension}/{fileNameWithoutExtension}-part-{partNumber:D4}.csv";
        }

        public string ProcessColumnDefinitions(char delimiter)
        {
            var columnList = s.ToLower().Split(delimiter).ToList();
            // Remove the first column which is assumed to be a redundant 'C' column
            columnList.RemoveAt(0);
            return string.Join(delimiter, columnList);
        }
    }
}