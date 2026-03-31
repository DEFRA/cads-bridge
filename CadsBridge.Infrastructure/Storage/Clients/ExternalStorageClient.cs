using CadsBridge.Infrastructure.Storage.Abstractions;

namespace CadsBridge.Infrastructure.Storage.Clients;

public class ExternalStorageClient : IStorageClient
{
    public string ClientName => GetType().Name;
}