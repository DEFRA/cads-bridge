using Amazon.S3;
using Amazon.S3.Model;
using CadsBridge.Application.DataLoad.Services;
using CadsBridge.Core.Exceptions;
using CadsBridge.Infrastructure.Storage.Abstractions;
using CadsBridge.Infrastructure.Storage.Clients;
using Microsoft.Extensions.Logging;
using System.Text;

namespace CadsBridge.Infrastructure.DataLoad.Services;

public class S3FileMetaDataService : IS3FileMetaDataService
{
    // 1 KB buffer for reading the trailer line
    private const int TailReadBytes = 1024;
    private readonly IAmazonS3? _s3;
    private readonly string _bucket;
    private readonly ILogger<S3FileMetaDataService> _logger;

    public S3FileMetaDataService(
        IS3ClientFactory s3ClientFactory,
        ILogger<S3FileMetaDataService> logger)
    {
        _logger = logger;
        var clientInfo = s3ClientFactory.GetClientInfo<InternalStorageClient>();
        _s3 = clientInfo.Client;
        _bucket = clientInfo.BucketName;
    }

    public async Task<long> GetRecordCountAsync(string s3Key, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(s3Key);



        // HEAD the object to get its size
        var fileSize = await GetFileSize(s3Key, cancellationToken);

        if (fileSize == 0)
        {
            throw new DomainException($"S3 object '{s3Key}' is empty; no trailer line present.");
        }

        // Extract the last line of the file
        var trailerLine = await GetLastLine(s3Key, fileSize, cancellationToken);

        if (string.IsNullOrWhiteSpace(trailerLine))
        {
            throw new DomainException($"Could not locate a trailer line in '{s3Key}'.");
        }

        if (_logger.IsEnabled(LogLevel.Debug))
        {
            _logger.LogDebug("Trailer line read from '{Key}': {Line}", s3Key, trailerLine);
        }

        // Parse and validate
        return ParseTrailerLine(trailerLine, s3Key);
    }

    private async Task<long> GetFileSize(string s3Key, CancellationToken cancellationToken)
    {
        try
        {
            var meta = await _s3!.GetObjectMetadataAsync(_bucket, s3Key, cancellationToken);
            return meta.ContentLength;
        }
        catch (AmazonS3Exception ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            throw new NotFoundException($"S3 object '{s3Key}' was not found in bucket '{_bucket}'.", ex);
        }
    }

    private async Task<string> GetLastLine(string s3Key, long fileSize, CancellationToken cancellationToken)
    {
        var rangeStart = Math.Max(0L, fileSize - TailReadBytes);

        var getRequest = new GetObjectRequest
        {
            BucketName = _bucket,
            Key = s3Key,
            ByteRange = new ByteRange(rangeStart, fileSize - 1)
        };
        try
        {
            using var response = await _s3!.GetObjectAsync(getRequest, cancellationToken);
            return ExtractLastLine(response.ResponseStream);
        }
        catch (AmazonS3Exception ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            throw new NotFoundException($"S3 object '{s3Key}' was not found in bucket '{_bucket}'.", ex);
        }
    }

    public static string ExtractLastLine(Stream stream)
    {
        using var reader = new StreamReader(stream, Encoding.UTF8);
        var text = reader.ReadToEnd();

        // Trailing line breaks (LF or CRLF) would otherwise yield an empty final entry.
        var lines = text.Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

        return lines.Length == 0 ? string.Empty : lines[^1];
    }

    public static long ParseTrailerLine(string line, string s3Key)
    {
        var expectedFileName = Path.GetFileName(s3Key);

        var parts = line.Split('|');

        if (parts.Length != 4)
        {
            throw new DomainException($"Trailer line in '{s3Key}' has {parts.Length} field(s); expected 4. Line: '{line}'");
        }
        if (!string.Equals(parts[0].Trim(), "T", StringComparison.Ordinal))
        {
            throw new DomainException($"Trailer line in '{s3Key}' does not begin with 'T'. Line: '{line}'");
        }
        var fileNameField = parts[1].Trim();
        if (!string.Equals(fileNameField, expectedFileName, StringComparison.OrdinalIgnoreCase))
        {
            throw new DomainException($"Trailer file name '{fileNameField}' does not match expected '{expectedFileName}' in '{s3Key}'.");
        }
        if (!long.TryParse(parts[3].Trim(), out var recordCount) || recordCount < 0)
        {
            throw new DomainException($"Trailer record count '{parts[3].Trim()}' in '{s3Key}' is not a valid non-negative integer.");
        }
        return recordCount;
    }
}