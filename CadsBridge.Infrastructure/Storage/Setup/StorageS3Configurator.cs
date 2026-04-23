using Amazon.S3;
using CadsBridge.Core.Storage.Abstractions;
using CadsBridge.Core.Storage.Clients;
using CadsBridge.Infrastructure.Storage.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace CadsBridge.Infrastructure.Storage.Setup;

public class StorageS3Configurator(StorageConfiguration config) : IConfigureS3Clients
{
    private readonly StorageConfiguration _config = config;

    public void Configure(IServiceProvider sp)
    {
        var factory = sp.GetRequiredService<IS3ClientFactory>();
        var amazonConfig = sp.GetRequiredService<AmazonS3Config>();

        factory.AddClient<InternalStorageClient>(
            _config.Internal.BucketName,
            amazonConfig);

        if (!string.IsNullOrEmpty(_config.External.AccessKeySecretName) &&
            !string.IsNullOrEmpty(_config.External.SecretKeySecretName))
        {
            factory.AddClientWithCredentials<ExternalStorageClient>(
                   _config.External.BucketName,
                   _config.External.AccessKeySecretName,
                   _config.External.SecretKeySecretName,
                   amazonConfig);
        }
        else
        {
            factory.AddClient<ExternalStorageClient>(
                _config.External.BucketName,
                amazonConfig);
        }
    }
}