namespace CadsBridge.Application.Models;

public record DataSeedImportJob(
    string JobId,
    string FileName,
    string TargetKey);