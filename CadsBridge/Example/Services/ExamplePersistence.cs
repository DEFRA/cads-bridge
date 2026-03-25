using CadsBridge.Example.Models;

namespace CadsBridge.Example.Services;

public interface IExamplePersistence
{
    public Task<bool> CreateAsync(ExampleModel example);

    public Task<ExampleModel?> GetByExampleName(string name);

    public Task<IEnumerable<ExampleModel>> GetAllAsync();

    public Task<IEnumerable<ExampleModel>> SearchByValueAsync(string searchTerm);

    public Task<bool> UpdateAsync(ExampleModel example);

    public Task<bool> DeleteAsync(string name);
}

public class FakePersistence : IExamplePersistence
{
    private readonly List<ExampleModel> _examples = new();

    public Task<bool> CreateAsync(ExampleModel example)
    {
        if (_examples.Any(e => e.Name.Equals(example.Name, StringComparison.OrdinalIgnoreCase)))
            return Task.FromResult(false);

        _examples.Add(example);
        return Task.FromResult(true);
    }

    public Task<ExampleModel?> GetByExampleName(string name)
    {
        var example = _examples.FirstOrDefault(e => e.Name.Equals(name, StringComparison.OrdinalIgnoreCase));
        return Task.FromResult(example);
    }

    public Task<IEnumerable<ExampleModel>> GetAllAsync()
    {
        return Task.FromResult(_examples.AsEnumerable());
    }

    public Task<IEnumerable<ExampleModel>> SearchByValueAsync(string searchTerm)
    {
        var matches = _examples.Where(e =>
            e.Name.Contains(searchTerm, StringComparison.OrdinalIgnoreCase) ||
            e.Value.Contains(searchTerm, StringComparison.OrdinalIgnoreCase));

        return Task.FromResult(matches);
    }

    public Task<bool> UpdateAsync(ExampleModel example)
    {
        var existing = _examples.FirstOrDefault(e => e.Name.Equals(example.Name, StringComparison.OrdinalIgnoreCase));
        if (existing is null) return Task.FromResult(false);

        existing.Value = example.Value;
        return Task.FromResult(true);
    }

    public Task<bool> DeleteAsync(string name)
    {
        var existing = _examples.FirstOrDefault(e => e.Name.Equals(name, StringComparison.OrdinalIgnoreCase));
        if (existing is null) return Task.FromResult(false);

        _examples.Remove(existing);
        return Task.FromResult(true);
    }
}