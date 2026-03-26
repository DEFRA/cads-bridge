using CadsBridge.Infrastructure.Storage.Abstractions;

namespace CadsBridge.Infrastructure.Storage.Clients;

public class ExternalClient : IStorageClient
{
    public string ClientName => GetType().Name;
}