using CadsBridge.Infrastructure.Storage.Abstractions;

namespace CadsBridge.Infrastructure.Storage.Clients;

public class InternalClient : IStorageClient
{
    public string ClientName => GetType().Name;
}