using Amazon.S3.Model;
using CadsBridge.Application.DataLoad.Scanning;
using CadsBridge.Application.DataLoad.Services.Testing;
using CadsBridge.Application.Encryption;
using CadsBridge.Application.Extensions;
using CadsBridge.Core.Attributes;
using CadsBridge.Infrastructure.DataLoad.Csv.Extensions;
using CadsBridge.Infrastructure.DataLoad.Sources.Parsers;
using CadsBridge.Infrastructure.Encryption.Configuration;
using CadsBridge.Infrastructure.Storage.Abstractions;
using CadsBridge.Infrastructure.Storage.Clients;
using CadsBridge.Infrastructure.Storage.Factories;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Globalization;
using System.IO.Pipelines;
using System.Net;
using System.Text;
using System.Text.RegularExpressions;

namespace CadsBridge.Infrastructure.DataLoad.Services.Testing;

public class InternalFileService(
    IS3ClientFactory s3ClientFactory,
    IFileEncryptor fileEncryptor,
    IOptions<EncryptionConfig> encryptionOptions,
    ILogger<InternalFileService> logger) : IInternalFileService
{
    private const string DataRecordPrefix = "D|";
    private static readonly Regex s_chunkNumberPattern = new(@"-part-(?<chunkNumber>\d{1,9})\.csv$",  RegexOptions.CultureInvariant);

    public async Task<IReadOnlyList<InternalFileChunk>> ListChunksAsync(string fileName, CancellationToken cancellationToken = default)
    {
        var folder = GetPartsFolder(fileName);
        var clientInfo = s3ClientFactory.GetClientInfo<InternalStorageClient>();

        var parts = await ListPartsAsync(clientInfo, folder, cancellationToken);

        var chunks = new List<InternalFileChunk>(parts.Count);
        foreach (var (chunkNo, s3Object) in parts.OrderBy(p => p.ChunkNo))
        {
            var recordCount = await CountDataRecordsAsync(clientInfo, s3Object.Key, cancellationToken);
            chunks.Add(new InternalFileChunk(s3Object.Key, chunkNo, s3Object.Size ?? 0, recordCount));
        }

        if (logger.IsEnabled(LogLevel.Information))
        {
            logger.LogInformation(
                "Listed {ChunkCount} internal chunks for test file {FileName} ({RecordCount} records)",
                chunks.Count, fileName, chunks.Sum(c => c.RecordCount));
        }

        return chunks;
    }

    internal static string GetPartsFolder(string fileName)
    {
        if (!CtsmFilenameParser.TryParse(fileName, out var parsed) || parsed is null || !TestingConstants.ValidE2EFilenamePattern.IsMatch(fileName))
        {
            throw new FormatException($"fileName '{fileName}' is not valid.");
        }

        var info = MapDataSourceInfo(fileName, parsed);
        var firstPartKey = $"{info.DestinationPrefix}/{fileName}".FormatSplitFileTargetKey();
        var finalPathSeparatorIndex = firstPartKey.LastIndexOf('/') + 1;
        return firstPartKey[..finalPathSeparatorIndex];
    }

    private static DataSourceTypeInfoAttribute MapDataSourceInfo(string fileName, CtsmFilename parsed)
    {
        var info = Enum.GetValues<DataSourceType>()
                       .Select(t => t.GetAttribute<DataSourceTypeInfoAttribute>())
                       .FirstOrDefault(attr => attr != null && string.Equals(attr.Name, parsed.Type, StringComparison.OrdinalIgnoreCase))
                   ?? throw new FormatException($"fileName '{fileName}' does not have a mapped data source type.");

        return info;
    }

    private static async Task<List<(int ChunkNo, S3Object Object)>> ListPartsAsync(
        S3ClientFactory.ClientInfo clientInfo,
        string folder,
        CancellationToken cancellationToken)
    {
        var result = new List<(int, S3Object)>();
        var request = new ListObjectsV2Request
        {
            BucketName = clientInfo.BucketName,
            Prefix = folder,
            Delimiter = "/"
        };

        ListObjectsV2Response response;

        do
        {
            response = await clientInfo.Client.ListObjectsV2Async(request, cancellationToken);

            if (response.HttpStatusCode != HttpStatusCode.OK)
            {
                throw new InvalidOperationException(
                    $"Failed to list '{folder}'. StatusCode: {response.HttpStatusCode}");
            }

            foreach (var s3Object in response.S3Objects ?? [])
            {
                if (TryGetChunkNumber(s3Object.Key, out var chunkNumber))
                {
                    result.Add((chunkNumber, s3Object));
                }
            }

            request.ContinuationToken = response.NextContinuationToken;
        } while (response.IsTruncated.GetValueOrDefault());

        return result;
    }

    private static bool TryGetChunkNumber(string key, out int chunkNumber)
    {
        chunkNumber = 0;

        var match = s_chunkNumberPattern.Match(key);
        if (!match.Success)
        {
            return false;
        }

        chunkNumber = int.Parse(match.Groups["chunkNumber"].Value, CultureInfo.InvariantCulture);
        return true;
    }

    private async Task<long> CountDataRecordsAsync(
        S3ClientFactory.ClientInfo clientInfo,
        string key,
        CancellationToken cancellationToken)
    {
        using var response = await clientInfo.Client.GetObjectAsync(clientInfo.BucketName, key, cancellationToken);

        if (!encryptionOptions.Value.StorageEncryptionEnabled)
        {
            return await CountDataRecordsAsync(response.ResponseStream, cancellationToken);
        }

        var pipe = new Pipe();
        var decryptTask = DecryptIntoPipeAsync(response.ResponseStream, pipe.Writer, cancellationToken);
        var countTask = CountFromPipeAsync(pipe.Reader, cancellationToken);

        await Task.WhenAll(decryptTask, countTask);
        return await countTask;
    }

    private async Task DecryptIntoPipeAsync(Stream encrypted, PipeWriter writer, CancellationToken cancellationToken)
    {
        try
        {
            await using var output = writer.AsStream(leaveOpen: true);
            await fileEncryptor.DecryptAsync(encrypted, output, cancellationToken: cancellationToken);
            await writer.CompleteAsync();
        }
        catch (Exception ex)
        {
            await writer.CompleteAsync(ex);
            throw;
        }
    }

    private static async Task<long> CountFromPipeAsync(PipeReader reader, CancellationToken cancellationToken)
    {
        try
        {
            await using var plaintext = reader.AsStream(leaveOpen: true);
            return await CountDataRecordsAsync(plaintext, cancellationToken);
        }
        finally
        {
            await reader.CompleteAsync();
        }
    }

    internal static async Task<long> CountDataRecordsAsync(Stream plaintext, CancellationToken cancellationToken)
    {
        using var reader = new StreamReader(plaintext, Encoding.UTF8, detectEncodingFromByteOrderMarks: true, leaveOpen: true);

        long count = 0;
        while (await reader.ReadLineAsync(cancellationToken) is { } line)
        {
            if (line.StartsWith(DataRecordPrefix, StringComparison.Ordinal))
            {
                count++;
            }
        }

        return count;
    }
}
