namespace CadsBridge.Application.DataLoad.Jobs;

public static class CsvDataFileImportJobExtensions
{
    extension(CsvDataFileImportJob job)
    {
        /// <summary>
        /// The internal storage key the file is copied to once decrypted.
        /// </summary>
        public string TargetKey => $"import/{Path.GetFileName(job.SourceKey)}";
    }
}

