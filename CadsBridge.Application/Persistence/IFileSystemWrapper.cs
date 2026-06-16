namespace CadsBridge.Application.Persistence;

public interface IFileSystemWrapper
{
    public Stream OpenRead(string path);
}