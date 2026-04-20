using Amazon.S3;
using CadsBridge.Core.Storage.Abstractions;

namespace CadsBridge.Core.Storage.Factories;

public class S3ClientFactory : IS3ClientFactory
{
    public sealed record ClientInfo(IAmazonS3 Client, string BucketName);

    private readonly Dictionary<string, ClientInfo> _clients = [];

    public IAmazonS3 GetClient<T>() where T : IStorageClient, new() => GetClientInfo<T>().Client;

    public IAmazonS3 GetClient(string clientName)
    {
        if (!_clients.TryGetValue(clientName, out var client))
            throw new KeyNotFoundException($"No S3 client registered for name '{clientName}'");

        return client.Client;
    }

    public ClientInfo GetClientInfo<T>()
        where T : IStorageClient, new()
    {
        var instance = new T();
        var storageClientName = instance.ClientName;

        if (!_clients.TryGetValue(storageClientName, out var client))
            throw new KeyNotFoundException($"No S3 client registered for name '{storageClientName}'");

        return client;
    }

    public string GetClientBucketName<T>() where T : IStorageClient, new() => GetClientInfo<T>().BucketName;

    public string GetClientBucketName(string clientName)
    {
        if (!_clients.TryGetValue(clientName, out var client))
            throw new KeyNotFoundException($"No S3 client registered for name '{clientName}'");

        return client.BucketName;
    }

    public IEnumerable<string> GetRegisteredClientNames() => _clients.Keys;

    public bool HasStorageClient(string storageClientName) => _clients.ContainsKey(storageClientName);

    public void AddClient<T>(string defaultBucketName, AmazonS3Config amazonS3Config)
        where T : IStorageClient, new()
    {
        var instance = new T();
        var storageClientName = instance.ClientName;

        if (!HasStorageClient(storageClientName))
        {
            var newClient = new AmazonS3Client(amazonS3Config);
            _clients[storageClientName] = new(newClient, defaultBucketName);
        }
    }

    public void AddClientWithCredentials<T>(string defaultBucketName, string accessKeyRef, string secretKeyRef, AmazonS3Config amazonS3Config)
        where T : IStorageClient, new()
    {
        var instance = new T();
        var storageClientName = instance.ClientName;

        if (!HasStorageClient(storageClientName))
        {
            var accessKey = Environment.GetEnvironmentVariable(accessKeyRef);
            var secretKey = Environment.GetEnvironmentVariable(secretKeyRef);

            if (string.IsNullOrWhiteSpace(defaultBucketName))
                throw new InvalidOperationException($"Missing bucket name for '{storageClientName}'");

            if (string.IsNullOrWhiteSpace(accessKey) || string.IsNullOrWhiteSpace(secretKey))
                throw new InvalidOperationException($"Missing AWS credentials for '{storageClientName}'");

            var newClient = new AmazonS3Client(accessKey, secretKey, amazonS3Config);
            _clients[storageClientName] = new(newClient, defaultBucketName);
        }
    }

    public void RegisterMockClient<T>(string bucketName, IAmazonS3 mockClient)
        where T : IStorageClient, new()
    {
        var instance = new T();
        var name = instance.ClientName;
        _clients[name] = new(mockClient, bucketName);
    }
}