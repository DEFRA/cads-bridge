namespace CadsBridge.Application.Messaging.Messages;

public class CsvDataFileImportMessage : MessageType
{
    public string Identifier { get; set; } = string.Empty;
}