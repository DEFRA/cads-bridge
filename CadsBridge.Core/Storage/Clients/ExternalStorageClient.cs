using CadsBridge.Core.Storage.Abstractions;

namespace CadsBridge.Core.Storage.Clients;

public class ExternalStorageClient : IStorageClient
{
    public string ClientName => GetType().Name;
}