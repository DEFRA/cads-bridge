namespace CadsBridge.Core.Correlation;

public sealed class CorrelationScope : IDisposable
{
    private readonly string? _previous;

    private CorrelationScope(string? previous) => _previous = previous;

    public static CorrelationScope Begin(string? correlationId)
    {
        var previous = CorrelationIdContext.Value;
        var id = string.IsNullOrWhiteSpace(correlationId) ? Guid.NewGuid().ToString() : correlationId;

        CorrelationIdContext.Value = id;

        return new CorrelationScope(previous);
    }

    public void Dispose() => CorrelationIdContext.Value = _previous;
}