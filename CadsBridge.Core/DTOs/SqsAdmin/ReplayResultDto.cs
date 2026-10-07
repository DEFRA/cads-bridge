namespace CadsBridge.Core.DTOs.SqsAdmin;

public record ReplayResultDto(int Moved, int Failed, IReadOnlyList<string> Errors);