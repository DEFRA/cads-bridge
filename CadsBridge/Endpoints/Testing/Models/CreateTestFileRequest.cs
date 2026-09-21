using CadsBridge.Application.DataLoad.Scanning;

namespace CadsBridge.Endpoints.Testing.Models;

public record CreateTestFileRequest(
    string FileName,
    string Content,
    DataSourceType DataSource,
    bool OverwriteExisting = false);