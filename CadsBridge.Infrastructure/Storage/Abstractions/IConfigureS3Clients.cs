namespace CadsBridge.Infrastructure.Storage.Abstractions;

public interface IConfigureS3Clients
{
    void Configure(IServiceProvider sp);
}