namespace CadsBridge.Core.Storage.FileSystem;

public interface IFileSytemWrapper
{
    public Stream OpenRead(string path);
}