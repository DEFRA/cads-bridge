using CadsBridge.Infrastructure.DataLoad.Csv.Files;

namespace CadsBridge.Infrastructure.DataLoad.Csv.Extensions;

public static class CtsmFilenameExtensions
{
    extension(CtsmFilename ctsmFilename)
    {
        public string DerivePassword()
        {
            var timeStamp = ctsmFilename.Timestamp.Substring(0, ctsmFilename.Timestamp.LastIndexOf('-'));
            var reversedTableName = string.Join("_", ctsmFilename.TableName.Split('_').Reverse());

            if (!string.IsNullOrWhiteSpace(ctsmFilename.PartNo))
            {
                return $"{timeStamp}_{reversedTableName}_{ctsmFilename.PartNo}_{ctsmFilename.BatchId}_{ctsmFilename.Type}_{ctsmFilename.Env}_{ctsmFilename.App}_CTSM";
            }
            else
            {
                return $"{timeStamp}_{reversedTableName}_{ctsmFilename.BatchId}_{ctsmFilename.Type}_{ctsmFilename.Env}_{ctsmFilename.App}_CTSM";
            }
        }
    }
}