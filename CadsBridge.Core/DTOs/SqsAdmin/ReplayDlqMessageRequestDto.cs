namespace CadsBridge.Core.DTOs.SqsAdmin;

public record ReplayDlqMessageRequestDto(string Queue, string MessageId);