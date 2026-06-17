using CadsBridge.Core.FileSystem;

namespace CadsBridge.Infrastructure.FileSystem;

public class FileSystemWrapper : IFileSystemWrapper
{
    public Stream OpenRead(string path)
    {
        return File.OpenRead(path);
    }
}