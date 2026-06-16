using CadsBridge.Application.Persistence;

namespace CadsBridge.Infrastructure.Persistence;

public class FileSystemWrapper : IFileSystemWrapper
{
    public Stream OpenRead(string path)
    {
        return File.OpenRead(path);
    }
}