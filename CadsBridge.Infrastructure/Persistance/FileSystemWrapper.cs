using CadsBridge.Application.Persistance;

namespace CadsBridge.Infrastructure.Persistance;

public class FileSystemWrapper : IFileSystemWrapper
{
    public Stream OpenRead(string path)
    {
        return File.OpenRead(path);
    }
}