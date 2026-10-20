namespace CadsBridge.Endpoints.SqsAdmin.Responses;

public record ReplayDlqMessageResponse(string Queue, string MessageId, int Moved, int Failed, IReadOnlyList<string> Errors);