using CadsBridge.Infrastructure.Storage.Abstractions;

namespace CadsBridge.Infrastructure.Storage.Clients;

public class InternalStorageClient : IStorageClient
{
    public string ClientName => GetType().Name;
}