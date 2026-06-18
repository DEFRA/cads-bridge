namespace CadsBridge.Core.FileSystem;

public interface IFileSystemWrapper
{
    Stream OpenRead(string path);
}