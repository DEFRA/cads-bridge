using Amazon.S3;
using Amazon.S3.Model;
using CadsBridge.Core.Storage.Abstractions;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

namespace CadsBridge.Infrastructure.Storage.Health;

public class AwsS3HealthCheck(IS3ClientFactory s3ClientFactory, ILogger<AwsS3HealthCheck> logger) : IHealthCheck
{
    private readonly IS3ClientFactory _s3ClientFactory = s3ClientFactory;

    public async Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        var results = new Dictionary<string, object>();
        var unhealthyClients = new List<string>();

        foreach (var clientName in _s3ClientFactory.GetRegisteredClientNames())
        {
            var client = _s3ClientFactory.GetClient(clientName);
            var bucketName = _s3ClientFactory.GetClientBucketName(clientName);

            try
            {
                var request = new ListObjectsV2Request
                {
                    BucketName = bucketName
                };

                var response = await client.ListObjectsV2Async(request, cancellationToken);

                if (response.HttpStatusCode == System.Net.HttpStatusCode.OK)
                {
                    results[clientName] = new
                    {
                        Bucket = bucketName,
                        Status = "Healthy"
                    };
                }
                else
                {
                    results[clientName] = new
                    {
                        Bucket = bucketName,
                        Status = $"Degraded (Status: {response.HttpStatusCode})"
                    };
                    unhealthyClients.Add(bucketName);
                }
            }
            catch (AmazonS3Exception ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
            {
                logger.LogError(ex, "AmazonS3Exception recieved when testing {BucketName}", bucketName);

                results[clientName] = new
                {
                    Bucket = bucketName,
                    Status = "Unhealthy (Bucket not found)",
                    Exception = ex.Message
                };
                unhealthyClients.Add(bucketName);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Exception recieved when testing {BucketName}", bucketName);

                results[clientName] = new
                {
                    Bucket = bucketName,
                    Status = "Unhealthy (Exception)",
                    Exception = ex.Message
                };
                unhealthyClients.Add(bucketName);
            }
        }

        if (unhealthyClients.Count == 0)
        {
            return HealthCheckResult.Healthy("All S3 buckets are reachable", results);
        }

        return HealthCheckResult.Unhealthy($"Some S3 buckets failed: {string.Join(", ", unhealthyClients)}", data: results);
    }
}