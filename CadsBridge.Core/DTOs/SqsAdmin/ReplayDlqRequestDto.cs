namespace CadsBridge.Core.DTOs.SqsAdmin;

public record ReplayDlqRequestDto(string Queue, int BatchSize);