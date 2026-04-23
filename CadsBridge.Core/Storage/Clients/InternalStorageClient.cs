using CadsBridge.Core.Storage.Abstractions;

namespace CadsBridge.Core.Storage.Clients;

public class InternalStorageClient : IStorageClient
{
    public string ClientName => GetType().Name;
}