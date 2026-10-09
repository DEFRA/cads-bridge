namespace CadsBridge.Endpoints.SqsAdmin.Responses;

public record ReplayDlqResponse(string Queue, int Moved, int Failed, IReadOnlyList<string> Errors);