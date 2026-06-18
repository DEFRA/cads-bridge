using CadsBridge.Core.FileSystem;
using System.Text;

namespace CadsBridge.Testing.Support.TestDoubles.FileSystem;

public class FakeFileSystem : IFileSystemWrapper
{
    private readonly Dictionary<string, string> _files = [];

    public void AddFile(string path, string content)
        => _files[path] = content;

    public Stream OpenRead(string path)
    {
        if (!_files.TryGetValue(path, out var content))
            throw new FileNotFoundException(path);

        return new MemoryStream(Encoding.UTF8.GetBytes(content));
    }
}