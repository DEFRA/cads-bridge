using System.Diagnostics.CodeAnalysis;
using System.Text.Json.Serialization;


namespace CadsBridge.Example.Models;

[ExcludeFromCodeCoverage]
public class ExampleModel
{
    public required string Name { get; set; }

    public required string Value { get; set; }

    public int? Counter { get; set; } = 0;

    public DateTime? Created { get; set; } = DateTime.UtcNow;
}