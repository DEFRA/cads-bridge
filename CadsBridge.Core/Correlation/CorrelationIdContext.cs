namespace CadsBridge.Core.Correlation;

public static class CorrelationIdContext
{
    private static readonly AsyncLocal<string?> s_correlationId = new();

    public static string? Value
    {
        get => s_correlationId.Value;
        set => s_correlationId.Value = value;
    }
}