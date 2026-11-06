using CadsBridge.Application.Encryption;
using CadsBridge.Infrastructure.Encryption.Configuration;
using CadsBridge.Infrastructure.Encryption.Providers;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace CadsBridge.Infrastructure.Encryption.Setup;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddEncryption(this IServiceCollection services, IConfiguration configuration)
    {
        services.AddOptions<EncryptionConfig>()
            .Bind(configuration.GetSection(EncryptionConfig.SectionName))
            .ValidateDataAnnotations()
            .ValidateOnStart();

        services.AddTransient<IMasterKeyProvider, InMemoryMasterKeyProvider>();
        services.AddTransient<IFileEncryptor, FileEncryptor>();

        return services;
    }
}