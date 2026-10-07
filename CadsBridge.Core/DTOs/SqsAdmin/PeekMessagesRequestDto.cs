namespace CadsBridge.Core.DTOs.SqsAdmin;

public record PeekMessagesRequestDto(string Queue, int MaxMessages);