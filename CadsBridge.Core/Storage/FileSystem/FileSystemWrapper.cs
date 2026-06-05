namespace CadsBridge.Core.Storage.FileSystem;

public class FileSystemWrapper : IFileSytemWrapper
{
    public Stream OpenRead(string path)
    {
        return File.OpenRead(path);
    }
}