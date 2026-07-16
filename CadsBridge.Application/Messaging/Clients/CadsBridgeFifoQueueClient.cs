namespace CadsBridge.Application.Messaging.Clients;

public class CadsBridgeFifoQueueClient : IQueueClient
{
    public string ClientName => GetType().Name;
}