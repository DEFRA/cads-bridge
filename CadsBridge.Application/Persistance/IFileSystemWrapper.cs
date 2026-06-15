namespace CadsBridge.Application.Persistance;

public interface IFileSystemWrapper
{
    public Stream OpenRead(string path);
}