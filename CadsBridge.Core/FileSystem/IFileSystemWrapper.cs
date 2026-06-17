namespace CadsBridge.Core.FileSystem;

public interface IFileSystemWrapper
{
    public Stream OpenRead(string path);
}