// using System.Globalization;
// using System.IO.Compression;
// using System.Security.Cryptography;
// using System.Text;
// using System.Text.Json;
// using System.Text.RegularExpressions;
// using Microsoft.VisualBasic.FileIO;
//
// namespace CadsBridge.Application.Services;
//
// /*
// Program flow
// How to run
//  - Build and run the dedicated export tool command: `dotnet run --project tools/CadsBridge -- run`
// - To target one bulk timestamp, pass `--data 2026-02-22-074603`.
// - Provide the required settings through `appsettings.json` and/or the `SANITISED_EXPORT_*` environment variables.
// - Make sure `AesSalt` is also configured because encrypted source files use the KRDS Bridge project AES convention.
//
// Parameters
// - `SANITISED_EXPORT_BUCKET_NAME`: vendor/source S3 bucket name.
// - `SANITISED_EXPORT_PREFIX`: optional source S3 prefix used to narrow the object listing.
// - `SANITISED_EXPORT_TARGET_BUCKET_NAME`: CADS/project S3 bucket name.
// - `SANITISED_EXPORT_TARGET_EXPORT_PREFIX`: S3 prefix for approved exports.
// - `SANITISED_EXPORT_TARGET_REJECTED_PREFIX`: S3 prefix for rejected outputs.
// - `SANITISED_EXPORT_TARGET_AUDIT_PREFIX`: S3 prefix for manifests, checksums, header reports, and mapping reports.
// - `SANITISED_EXPORT_REGION`: AWS region for the bucket, defaults to `eu-west-2`.
// - `SANITISED_EXPORT_SERVICE_URL`: optional S3-compatible endpoint, mainly for LocalStack or non-AWS endpoints.
// - `SANITISED_EXPORT_ACCESS_KEY`: optional access key for explicit S3 credentials.
// - `SANITISED_EXPORT_SECRET_KEY`: optional secret key for explicit S3 credentials.
// - `SANITISED_EXPORT_SAMPLE_SIZE`: analysis sample size used while scanning source files.
// - `SANITISED_EXPORT_EXPORT_RECORD_COUNT`: number of faker rows to export for non-reference outputs.
// - `SANITISED_EXPORT_SOURCE_FILES_ENCRYPTED`: set to `true` or `false` depending on whether source files need AES decryption.
// - `SANITISED_EXPORT_REFERENCE_TABLE_PATTERNS`: optional comma-separated list of patterns treated as reference data.
// - `SANITISED_EXPORT_APPROVED_OUTPUT_PATTERNS`: optional comma-separated allow-list for final output file names.
// - `SANITISED_EXPORT_CTS_TABLES_SQL_PATH`: path to the CTS table definition SQL file used for header checks.
// - `SANITISED_EXPORT_GENERATE_HEADER_REPORT`: set to `true` or `false` to write the CSV-vs-table header report.
// - `SANITISED_EXPORT_IMPORT_TO_POSTGRES`: set to `true` or `false` to import approved outputs into Postgres.
// - `SANITISED_EXPORT_POSTGRES_CONNECTION_STRING`: Postgres connection string used when import is enabled.
// - `SANITISED_EXPORT_POSTGRES_SCHEMA`: optional target schema, defaults to `public`.
// - `SanitisedExport:FieldMappings`: optional file-pattern/column/data-type rules used to drive anonymisation.
// - `AesSalt`: salt used by the KRDS Bridge project AES decryption convention.
//
// What happens
// 1. Load the export settings.
// 2. Connect to the source S3 bucket using the same storage approach as used within KRDS Bridge project.
// 3. List the candidate objects under the configured prefix.
// 4. For each object, stream it down, decrypt it if required, and handle gzip in-stream.
// 5. For each flat data file, classify it, analyse it, then either copy it through or anonymise the sample rows using the configured field mappings.
// 6. While anonymising, keep an in-memory original-to-fake lookup so repeated values stay consistent within the same run.
// 7. Compare the CSV headers against the expected CTS table definition and write a report of missing and extra columns.
// 8. Validate the generated CSV and upload it to the approved or rejected S3 prefix.
// 9. Optionally import approved files into Postgres.
// 10. Upload the manifest, checksum, header report, and mapping report to the audit S3 prefix.
// 11. No decrypted or staged files are written to local disk.
// */
// internal sealed class SanitizedExportRunner(
//     IConfiguration configuration,
//     IAesCryptoTransform aesCryptoTransform,
//     IPasswordSaltService passwordSaltService,
//     ILogger<SanitizedExportRunner> logger)
// {
//     private readonly IConfiguration _configuration = configuration;
//     private readonly IAesCryptoTransform _aesCryptoTransform = aesCryptoTransform;
//     private readonly IPasswordSaltService _passwordSaltService = passwordSaltService;
//     private readonly ILogger<SanitizedExportRunner> _logger = logger;
//
//     // Entry point for the command. This coordinates the whole run from source discovery through to audit upload.
//     public async Task<int> RunAsync(string? data, CancellationToken cancellationToken)
//     {
//         var options = LoadOptions();
//         options.Normalise();
//         options.Validate();
//         _logger.LogInformation("Configuration loaded and validated successfully.");
//         var valueMappingStore = ValueMappingStore.CreateInMemory(_logger);
//         var tableDefinitions = CtsTableDefinitionLoader.Load(options.CtsTablesSqlPath, _logger);
//
//         try
//         {
//             _logger.LogInformation("Starting sanitised export run.");
//             _logger.LogInformation(
//                 "Effective settings: SourceBucket={SourceBucket}, SourcePrefix={SourcePrefix}, TargetBucket={TargetBucket}, TargetExportPrefix={TargetExportPrefix}, TargetRejectedPrefix={TargetRejectedPrefix}, TargetAuditPrefix={TargetAuditPrefix}, Region={Region}, ServiceUrl={ServiceUrl}, SampleSize={SampleSize}, ExportRecordCount={ExportRecordCount}, SourceFilesEncrypted={SourceFilesEncrypted}, CtsTablesSqlPath={CtsTablesSqlPath}, GenerateHeaderReport={GenerateHeaderReport}, ImportToPostgres={ImportToPostgres}, PostgresSchema={PostgresSchema}, Data={Data}",
//                 options.BucketName,
//                 string.IsNullOrWhiteSpace(options.Prefix) ? "(none)" : options.Prefix,
//                 options.TargetBucketName,
//                 options.TargetExportPrefix,
//                 options.TargetRejectedPrefix,
//                 options.TargetAuditPrefix,
//                 options.Region,
//                 string.IsNullOrWhiteSpace(options.ServiceUrl) ? "(aws-default)" : options.ServiceUrl,
//                 options.SampleSize,
//                 options.ExportRecordCount,
//                 options.SourceFilesEncrypted,
//                 options.CtsTablesSqlPath,
//                 options.GenerateHeaderReport,
//                 options.ImportToPostgres,
//                 options.PostgresSchema,
//                 string.IsNullOrWhiteSpace(data) ? "(all)" : data);
//             _logger.LogInformation(
//                 "Pattern settings: ReferenceTablePatterns={ReferencePatterns}; ApprovedOutputPatterns={ApprovedPatterns}; FieldMappings={FieldMappings}",
//                 string.Join(", ", options.ReferenceTablePatterns),
//                 string.Join(", ", options.ApprovedOutputPatterns),
//                 options.FieldMappings.Count == 0
//                     ? "(default header rules)"
//                     : string.Join(" | ", options.FieldMappings.Select(mapping => $"{mapping.FilePattern ?? "*"}:{mapping.ColumnName}:{mapping.DataType}")));
//             _logger.LogInformation(
//                 "Postgres import is {Status}. Anonymised and approved CSV files are uploaded to s3://{TargetBucket}/{TargetPrefix}/{RunIdPlaceholder}.",
//                 options.ImportToPostgres ? "enabled" : "disabled",
//                 options.TargetBucketName,
//                 options.TargetExportPrefix,
//                 "{runId}");
//
//             var runId = DateTime.UtcNow.ToString("yyyyMMddHHmmss", CultureInfo.InvariantCulture);
//             _logger.LogInformation("Run id: {RunId}", runId);
//
//             // Keep the S3 access aligned with the approach used within KRDS Bridge project rather than dropping down to raw SDK calls.
//             using var sourceStorage = CreateSourceStorage(options);
//             using var targetStorage = CreateTargetStorage(options);
//             var objectInfos = await sourceStorage.ListAsync(options.Prefix, cancellationToken);
//             objectInfos = FilterObjectsByDataToken(objectInfos, data);
//             if (objectInfos.Count == 0)
//             {
//                 _logger.LogWarning("No S3 objects found in bucket {Bucket} with prefix {Prefix}{DataFilter}.",
//                     options.BucketName,
//                     options.Prefix,
//                     string.IsNullOrWhiteSpace(data) ? string.Empty : $" and data token {data}");
//                 return 0;
//             }
//
//             _logger.LogInformation("Discovered {Count} S3 object(s) to process{DataFilter}.",
//                 objectInfos.Count,
//                 string.IsNullOrWhiteSpace(data) ? string.Empty : $" for data token {data}");
//
//             var orderedObjectInfos = OrderObjectsForKnownCtsmBulkRun(objectInfos);
//             _logger.LogInformation("Object processing order prepared for {Count} object(s).", orderedObjectInfos.Count);
//
//             var manifestEntries = new List<ExportManifestEntry>();
//             var headerReports = new List<HeaderComparisonReportEntry>();
//             foreach (var objectInfo in orderedObjectInfos)
//             {
//                 _logger.LogInformation("Starting processing for source object {ObjectKey}.", objectInfo.Key);
//                 try
//                 {
//                     var result = await ProcessSourceObjectStreamingAsync(sourceStorage, targetStorage, objectInfo, options, valueMappingStore, tableDefinitions, runId, cancellationToken);
//                     headerReports.Add(result.HeaderReport);
//                     if (options.ImportToPostgres && result.Entry.Approved && result.TargetObjectKey is not null)
//                     {
//                         await ImportApprovedObjectToPostgresAsync(targetStorage, result.TargetObjectKey, result.HeaderReport, options, cancellationToken);
//                     }
//                     manifestEntries.Add(result.Entry);
//
//                     _logger.LogInformation("Completed processing for source object {ObjectKey}.", objectInfo.Key);
//                 }
//                 catch (Exception ex)
//                 {
//                     _logger.LogError(ex, "Failed to process S3 object {ObjectKey}.", objectInfo.Key);
//                     manifestEntries.Add(ExportManifestEntry.Failed(objectInfo.Key, ex.Message));
//                 }
//             }
//
//             await UploadAuditFilesAsync(targetStorage, manifestEntries, options, runId, cancellationToken);
//             if (options.GenerateHeaderReport)
//             {
//                 await UploadHeaderReportAsync(targetStorage, headerReports, options, runId, cancellationToken);
//             }
//             await UploadMappingReportAsync(targetStorage, valueMappingStore, options, runId, cancellationToken);
//             _logger.LogInformation(
//                 "Mapping store summary: TotalEntries={TotalEntries}, ReusedValues={ReusedValues}, NewValues={NewValues}.",
//                 valueMappingStore.TotalEntries,
//                 valueMappingStore.ReusedValues,
//                 valueMappingStore.NewValues);
//
//             var approvedCount = manifestEntries.Count(entry => entry.Approved);
//             var rejectedCount = manifestEntries.Count(entry => !entry.Approved);
//             _logger.LogInformation("Run completed. Approved: {Approved}. Rejected: {Rejected}.", approvedCount, rejectedCount);
//             return rejectedCount == 0 ? 0 : 1;
//         }
//         finally
//         {
//             _logger.LogInformation("Sanitised export run finished.");
//         }
//     }
//
//     // When a bulk timestamp is supplied, only keep files that contain that exact token in the file name.
//     private IReadOnlyList<KeeperData.Core.Storage.Dtos.StorageObjectInfo> FilterObjectsByDataToken(
//         IReadOnlyList<KeeperData.Core.Storage.Dtos.StorageObjectInfo> objectInfos,
//         string? data)
//     {
//         if (string.IsNullOrWhiteSpace(data))
//         {
//             return objectInfos;
//         }
//
//         var filtered = objectInfos
//             .Where(item => FileNameContainsDataToken(item.Key, data))
//             .ToList();
//
//         _logger.LogInformation("Filtered object list from {OriginalCount} to {FilteredCount} using data token {Data}.",
//             objectInfos.Count,
//             filtered.Count,
//             data);
//         _logger.LogDebug("Filtered object keys: {ObjectKeys}", string.Join(", ", filtered.Select(item => item.Key)));
//
//         return filtered;
//     }
//
//     // Match against the file name so prefixes and nested paths do not affect the selection.
//     private static bool FileNameContainsDataToken(string objectKey, string data)
//     {
//         var fileName = Path.GetFileName(objectKey);
//         return fileName.Contains($"_{data}.", StringComparison.OrdinalIgnoreCase)
//             || fileName.Contains($"_{data}_", StringComparison.OrdinalIgnoreCase)
//             || fileName.Contains(data, StringComparison.OrdinalIgnoreCase);
//     }
//
//     // Process the known CTSM bulk files in a stable order first, then fall back to alpha order for anything unexpected.
//     private IReadOnlyList<KeeperData.Core.Storage.Dtos.StorageObjectInfo> OrderObjectsForKnownCtsmBulkRun(
//         IReadOnlyList<KeeperData.Core.Storage.Dtos.StorageObjectInfo> objectInfos)
//     {
//         var runOrder = GetKnownCtsmBulkFilePatterns()
//             .Select((pattern, index) => new { pattern, index })
//             .ToDictionary(item => item.pattern, item => item.index, StringComparer.OrdinalIgnoreCase);
//
//         var ordered = objectInfos
//             .OrderBy(item => GetKnownRunOrderIndex(item.Key, runOrder))
//             .ThenBy(item => item.Key, StringComparer.OrdinalIgnoreCase)
//             .ToList();
//
//         _logger.LogDebug("Ordered object keys: {ObjectKeys}", string.Join(", ", ordered.Select(item => item.Key)));
//         return ordered;
//     }
//
//     // Keep the expected file sequence in one place so it is easy to update when the bulk drop changes.
//     private static IReadOnlyList<string> GetKnownCtsmBulkFilePatterns() =>
//     [
//         // 0.00 MB
//         "CT_LOCATION_TYPES",
//         // 0.00 MB
//         "CT_LOC_TYPE_REL_COMBS",
//         // 0.00 MB
//         "CT_LOCATION_ID_FORMATS",
//         // 0.00 MB
//         "CT_CONDITION_TYPES",
//         // 0.00 MB
//         "CT_EARTAG_TYPES",
//         // 0.00 MB
//         "CT_EARTAG_REASONS",
//         // 0.00 MB
//         "CT_EARTAG_FORMATS",
//         // 0.00 MB
//         "CT_PROBITY_CHECKS",
//         // 0.00 MB
//         "CT_SCHEMES",
//         // 0.00 MB
//         "CT_CM_AUTHORITIES",
//         // 0.00 MB
//         "CT_COND_VARIANT_GROUPINGS",
//         // 0.01 MB
//         "CT_LOCATION_REL_TYPES",
//         // 0.01 MB
//         "CT_COUNTRIES",
//         // 0.01 MB
//         "CT_COUNTIES",
//         // 0.01 MB
//         "CT_PARAM_HEADER",
//         // 0.01 MB
//         "CT_CONDITIONS",
//         // 0.03 MB
//         "CT_PARAM_GROUP",
//         // 0.06 MB
//         "CT_RECEIVED_APPLICATIONS",
//         // 0.06 MB
//         "CT_CONDITION_VARIANTS",
//         // 0.07 MB
//         "CT_PARAM_VALUE_GROUP",
//         // 0.12 MB
//         "CT_SUSP_CM_MEASURE_RESULTS",
//         // 0.12 MB
//         "CT_SUSP_CONDITION_MARKERS",
//         // 0.16 MB
//         "CT_BREEDS",
//         // 0.19 MB
//         "CT_CONDITION_ACTIVITIES",
//         // 0.83 MB
//         "CT_LOCATION_PARTY_RELS",
//         // 2.45 MB
//         "CT_PARAM_VALUE",
//         // 3.35 MB
//         "CT_RECEIVED_MOVEMENTS",
//         // 7.04 MB
//         "CT_SUSPENDED_ANIMALS",
//         // 18.3 MB
//         "CT_LOCATION_RELATIONSHIPS",
//         // 18.3 MB
//         "CT_LOCATION_PARTY_REL_TYPES",
//         // 31.0 MB
//         "CT_LOCATION_IDENTIFIERS",
//         // 34.3 MB
//         "CT_SUSPENDED_MOVEMENTS",
//         // 35.9 MB
//         "CT_CM_MEASURES_RESULTS",
//         // 44.6 MB
//         "CT_PARTIES",
//         // 330.6 MB
//         "CT_CONDITION_MARKERS",
//         // 1,020 MB
//         "CT_REGISTERED_ANIMALS",
//         // 5,800 MB
//         "CT_ISSUED_DOCUMENTS",
//         // 5,800 MB
//         "CT_LOCATIONS",
//         // 11,000 MB
//         "CT_VALID_APPLICATIONS",
//         // 13,000 MB
//         "CT_EARTAGS",
//         // 14,800 MB
//         "CT_ANIMAL_RELATIONSHIPS",
//         // 21,000 MB
//         "CT_ANIMAL_STATUSES",
//         // 83,200 MB
//         "CT_REGISTERED_MOVEMENTS"
//     ];
//
//     // Match on the table token rather than the full filename so the order still works when the timestamp changes.
//     private static int GetKnownRunOrderIndex(string objectKey, IReadOnlyDictionary<string, int> runOrder)
//     {
//         foreach (var entry in runOrder)
//         {
//             if (objectKey.Contains(entry.Key, StringComparison.OrdinalIgnoreCase))
//             {
//                 return entry.Value;
//             }
//         }
//
//         return int.MaxValue;
//     }
//
//     // Pull settings from config first, then allow explicit SANITISED_EXPORT_* vars to override them.
//     // Legacy SANITIZED_EXPORT_* names are still accepted so existing runs do not break.
//     // Field mappings are expected to come from the SanitisedExport config section, with SanitizedExport kept as a fallback alias.
//     private SanitizedExportOptions LoadOptions()
//     {
//         var options = new SanitizedExportOptions();
//         _configuration.GetSection("SanitisedExport").Bind(options);
//         _configuration.GetSection("SanitizedExport").Bind(options);
//
//         ApplyEnvironmentOverride(["SANITISED_EXPORT_BUCKET_NAME", "SANITIZED_EXPORT_BUCKET_NAME"], value => options.BucketName = value);
//         ApplyEnvironmentOverride(["SANITISED_EXPORT_PREFIX", "SANITIZED_EXPORT_PREFIX"], value => options.Prefix = value);
//         ApplyEnvironmentOverride(["SANITISED_EXPORT_REGION", "SANITIZED_EXPORT_REGION"], value => options.Region = value);
//         ApplyEnvironmentOverride(["SANITISED_EXPORT_SERVICE_URL", "SANITIZED_EXPORT_SERVICE_URL"], value => options.ServiceUrl = value);
//         ApplyEnvironmentOverride(["SANITISED_EXPORT_ACCESS_KEY", "SANITIZED_EXPORT_ACCESS_KEY"], value => options.AccessKey = value);
//         ApplyEnvironmentOverride(["SANITISED_EXPORT_SECRET_KEY", "SANITIZED_EXPORT_SECRET_KEY"], value => options.SecretKey = value);
//         ApplyEnvironmentOverride(["SANITISED_EXPORT_TARGET_BUCKET_NAME", "SANITIZED_EXPORT_TARGET_BUCKET_NAME"], value => options.TargetBucketName = value);
//         ApplyEnvironmentOverride(["SANITISED_EXPORT_TARGET_EXPORT_PREFIX", "SANITIZED_EXPORT_TARGET_EXPORT_PREFIX"], value => options.TargetExportPrefix = value);
//         ApplyEnvironmentOverride(["SANITISED_EXPORT_TARGET_REJECTED_PREFIX", "SANITIZED_EXPORT_TARGET_REJECTED_PREFIX"], value => options.TargetRejectedPrefix = value);
//         ApplyEnvironmentOverride(["SANITISED_EXPORT_TARGET_AUDIT_PREFIX", "SANITIZED_EXPORT_TARGET_AUDIT_PREFIX"], value => options.TargetAuditPrefix = value);
//         ApplyEnvironmentOverride(["SANITISED_EXPORT_SAMPLE_SIZE", "SANITIZED_EXPORT_SAMPLE_SIZE"], value => options.SampleSize = int.Parse(value, CultureInfo.InvariantCulture));
//         ApplyEnvironmentOverride(["SANITISED_EXPORT_EXPORT_RECORD_COUNT", "SANITIZED_EXPORT_EXPORT_RECORD_COUNT"], value => options.ExportRecordCount = int.Parse(value, CultureInfo.InvariantCulture));
//         ApplyEnvironmentOverride(["SANITISED_EXPORT_SOURCE_FILES_ENCRYPTED", "SANITIZED_EXPORT_SOURCE_FILES_ENCRYPTED"], value => options.SourceFilesEncrypted = bool.Parse(value));
//         ApplyEnvironmentOverride(["SANITISED_EXPORT_CTS_TABLES_SQL_PATH", "SANITIZED_EXPORT_CTS_TABLES_SQL_PATH"], value => options.CtsTablesSqlPath = value);
//         ApplyEnvironmentOverride(["SANITISED_EXPORT_GENERATE_HEADER_REPORT", "SANITIZED_EXPORT_GENERATE_HEADER_REPORT"], value => options.GenerateHeaderReport = bool.Parse(value));
//         ApplyEnvironmentOverride(["SANITISED_EXPORT_IMPORT_TO_POSTGRES", "SANITIZED_EXPORT_IMPORT_TO_POSTGRES"], value => options.ImportToPostgres = bool.Parse(value));
//         ApplyEnvironmentOverride(["SANITISED_EXPORT_POSTGRES_CONNECTION_STRING", "SANITIZED_EXPORT_POSTGRES_CONNECTION_STRING"], value => options.PostgresConnectionString = value);
//         ApplyEnvironmentOverride(["SANITISED_EXPORT_POSTGRES_SCHEMA", "SANITIZED_EXPORT_POSTGRES_SCHEMA"], value => options.PostgresSchema = value);
//         ApplyEnvironmentOverride(["SANITISED_EXPORT_REFERENCE_TABLE_PATTERNS", "SANITIZED_EXPORT_REFERENCE_TABLE_PATTERNS"], value => options.ReferenceTablePatterns = SplitList(value));
//         ApplyEnvironmentOverride(["SANITISED_EXPORT_APPROVED_OUTPUT_PATTERNS", "SANITIZED_EXPORT_APPROVED_OUTPUT_PATTERNS"], value => options.ApprovedOutputPatterns = SplitList(value));
//
//         _logger.LogDebug("Raw configuration section bound for SanitisedExport/SanitizedExport.");
//
//         return options;
//     }
//
//     // Small helper so the environment override code stays readable.
//     private void ApplyEnvironmentOverride(IEnumerable<string> variableNames, Action<string> apply)
//     {
//         foreach (var variableName in variableNames)
//         {
//             var value = Environment.GetEnvironmentVariable(variableName);
//             if (!string.IsNullOrWhiteSpace(value))
//             {
//                 apply(value);
//                 return;
//             }
//         }
//     }
//
//     // Allow simple comma-separated pattern lists from environment variables.
//     private static List<string> SplitList(string value) =>
//         value.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).ToList();
//
//     // Build the read-only S3 client with the same options the rest of the repo expects.
//     private S3BlobStorageServiceReadOnly CreateSourceStorage(SanitizedExportOptions options)
//     {
//         _logger.LogInformation("Creating source storage client for bucket {Bucket}.", options.BucketName);
//
//         if (!string.IsNullOrWhiteSpace(options.ServiceUrl)
//             && !string.IsNullOrWhiteSpace(options.AccessKey)
//             && !string.IsNullOrWhiteSpace(options.SecretKey))
//         {
//             _logger.LogInformation("Using explicit service URL and explicit credentials for source storage.");
//             return new S3BlobStorageServiceReadOnly(
//                 options.ServiceUrl,
//                 options.AccessKey,
//                 options.SecretKey,
//                 _logger,
//                 options.BucketName);
//         }
//
//         if (!string.IsNullOrWhiteSpace(options.AccessKey) && !string.IsNullOrWhiteSpace(options.SecretKey))
//         {
//             _logger.LogInformation("Using AWS region configuration with explicit credentials for source storage.");
//             return new S3BlobStorageServiceReadOnly(
//                 options.AccessKey,
//                 options.SecretKey,
//                 new Amazon.S3.AmazonS3Config
//                 {
//                     RegionEndpoint = Amazon.RegionEndpoint.GetBySystemName(options.Region),
//                     ServiceURL = options.ServiceUrl,
//                     ForcePathStyle = !string.IsNullOrWhiteSpace(options.ServiceUrl)
//                 },
//                 _logger,
//                 options.BucketName);
//         }
//
//         if (!string.IsNullOrWhiteSpace(options.ServiceUrl))
//         {
//             _logger.LogInformation("Using service URL without explicit credentials for source storage.");
//             return new S3BlobStorageServiceReadOnly(
//                 new Amazon.S3.AmazonS3Config
//                 {
//                     ServiceURL = options.ServiceUrl,
//                     ForcePathStyle = true
//                 },
//                 _logger,
//                 options.BucketName);
//         }
//
//         return new S3BlobStorageServiceReadOnly(
//             Amazon.RegionEndpoint.GetBySystemName(options.Region),
//             _logger,
//             options.BucketName);
//     }
//
//     // Build the writable CADS/project S3 target using the same storage abstractions used in Keeper.
//     private S3BlobStorageService CreateTargetStorage(SanitizedExportOptions options)
//     {
//         _logger.LogInformation("Creating target storage client for bucket {Bucket}.", options.TargetBucketName);
//
//         if (!string.IsNullOrWhiteSpace(options.ServiceUrl)
//             && !string.IsNullOrWhiteSpace(options.AccessKey)
//             && !string.IsNullOrWhiteSpace(options.SecretKey))
//         {
//             return new S3BlobStorageService(
//                 options.ServiceUrl,
//                 options.AccessKey,
//                 options.SecretKey,
//                 CreateTargetLogger(),
//                 options.TargetBucketName);
//         }
//
//         if (!string.IsNullOrWhiteSpace(options.AccessKey) && !string.IsNullOrWhiteSpace(options.SecretKey))
//         {
//             return new S3BlobStorageService(
//                 options.AccessKey,
//                 options.SecretKey,
//                 new Amazon.S3.AmazonS3Config
//                 {
//                     RegionEndpoint = Amazon.RegionEndpoint.GetBySystemName(options.Region),
//                     ServiceURL = options.ServiceUrl,
//                     ForcePathStyle = !string.IsNullOrWhiteSpace(options.ServiceUrl)
//                 },
//                 CreateTargetLogger(),
//                 options.TargetBucketName);
//         }
//
//         if (!string.IsNullOrWhiteSpace(options.ServiceUrl))
//         {
//             return new S3BlobStorageService(
//                 new Amazon.S3.AmazonS3Config
//                 {
//                     ServiceURL = options.ServiceUrl,
//                     ForcePathStyle = true
//                 },
//                 CreateTargetLogger(),
//                 options.TargetBucketName);
//         }
//
//         return new S3BlobStorageService(
//             Amazon.RegionEndpoint.GetBySystemName(options.Region),
//             CreateTargetLogger(),
//             options.TargetBucketName);
//     }
//
//     private ILogger<S3BlobStorageService> CreateTargetLogger()
//     {
//         var factory = LoggerFactory.Create(builder => builder.AddConsole());
//         return factory.CreateLogger<S3BlobStorageService>();
//     }
//
//     // Keeper-style streaming path: read from vendor S3, transform in-flight, and upload directly to the CADS bucket.
//     private async Task<StreamingProcessedFileResult> ProcessSourceObjectStreamingAsync(
//         IBlobStorageServiceReadOnly sourceStorage,
//         IBlobStorageService targetStorage,
//         KeeperData.Core.Storage.Dtos.StorageObjectInfo objectInfo,
//         SanitizedExportOptions options,
//         ValueMappingStore valueMappingStore,
//         IReadOnlyDictionary<string, CtsTableDefinition> tableDefinitions,
//         string runId,
//         CancellationToken cancellationToken)
//     {
//         var analysis = await AnalyseSourceObjectAsync(sourceStorage, objectInfo, options, cancellationToken);
//         var targetTableName = GetTargetTableName(Path.GetFileName(objectInfo.Key));
//         var headerReport = CompareHeaders(targetTableName, analysis.Headers, tableDefinitions, objectInfo.Key, Path.GetFileName(objectInfo.Key));
//         var outputName = BuildOutputName(objectInfo.Key, Path.GetFileName(objectInfo.Key));
//
//         if (analysis.IsZipArchive)
//         {
//             var rejectedKey = CombineS3Key(options.TargetRejectedPrefix, runId, outputName);
//             var rejectionReason = "Zip archives are not supported in the zero-local-storage streaming mode.";
//             await UploadTextAsync(targetStorage, rejectedKey, rejectionReason, cancellationToken);
//             return new StreamingProcessedFileResult(
//                 ExportManifestEntry.Rejected(objectInfo.Key, objectInfo.StorageUri.ToString(), BuildS3Uri(options.TargetBucketName, rejectedKey), TableKind.NonReference, analysis.ToFileAnalysis(), rejectionReason),
//                 headerReport,
//                 rejectedKey);
//         }
//
//         var tableKind = FileClassifier.Classify(Path.GetFileName(objectInfo.Key), options.ReferenceTablePatterns);
//         ValidationResult validation;
//         string targetKey;
//
//         if (tableKind == TableKind.Reference)
//         {
//             validation = OutputValidator.ValidateGenerated(analysis.Headers, [], tableKind, analysis.ToFileAnalysis(), 0);
//             targetKey = CombineS3Key(
//                 validation.IsApproved && OutputPolicy.IsAllowed(outputName, options.ApprovedOutputPatterns) ? options.TargetExportPrefix : options.TargetRejectedPrefix,
//                 runId,
//                 outputName);
//             _logger.LogInformation("Reference CSV {ObjectKey} will be uploaded to s3://{Bucket}/{TargetKey}.", objectInfo.Key, options.TargetBucketName, targetKey);
//             await StreamReferenceCsvToTargetAsync(sourceStorage, targetStorage, objectInfo, analysis.Delimiter, targetKey, options, cancellationToken);
//         }
//         else
//         {
//             var anonymisedRows = analysis.ExportRows
//                 .Select(row => RowAnonymizer.Anonymize(Path.GetFileName(objectInfo.Key), analysis.Headers, row, options.FieldMappings, valueMappingStore))
//                 .ToList();
//
//             validation = OutputValidator.ValidateGenerated(analysis.Headers, anonymisedRows, tableKind, analysis.ToFileAnalysis(), anonymisedRows.Count);
//             targetKey = CombineS3Key(
//                 validation.IsApproved && OutputPolicy.IsAllowed(outputName, options.ApprovedOutputPatterns) ? options.TargetExportPrefix : options.TargetRejectedPrefix,
//                 runId,
//                 outputName);
//             _logger.LogInformation("Anonymised CSV {ObjectKey} will be uploaded to s3://{Bucket}/{TargetKey}.", objectInfo.Key, options.TargetBucketName, targetKey);
//             await UploadGeneratedCsvAsync(targetStorage, targetKey, analysis.Headers, anonymisedRows, cancellationToken);
//         }
//
//         var approved = validation.IsApproved && OutputPolicy.IsAllowed(outputName, options.ApprovedOutputPatterns);
//         if (!approved)
//         {
//             var rejectionReason = validation.IsApproved
//                 ? $"Output file '{outputName}' is not in the approved allow-list."
//                 : string.Join("; ", validation.Messages);
//             _logger.LogWarning("Rejected {ObjectKey}: {Reason}", objectInfo.Key, rejectionReason);
//         }
//
//         var targetUri = BuildS3Uri(options.TargetBucketName, targetKey);
//         var checksum = await ComputeSha256ForObjectAsync(targetStorage, targetKey, cancellationToken);
//
//         var entry = approved
//             ? ExportManifestEntry.ApprovedEntry(objectInfo.Key, objectInfo.StorageUri.ToString(), targetUri, tableKind, analysis.ToFileAnalysis(), checksum, validation.Messages, objectInfo.Size, objectInfo.ETag)
//             : ExportManifestEntry.Rejected(objectInfo.Key, objectInfo.StorageUri.ToString(), targetUri, tableKind, analysis.ToFileAnalysis(), string.Join("; ", validation.Messages.DefaultIfEmpty("Output failed validation.")));
//
//         return new StreamingProcessedFileResult(entry, headerReport, targetKey);
//     }
//
//     private async Task<StreamingFileAnalysis> AnalyseSourceObjectAsync(
//         IBlobStorageServiceReadOnly sourceStorage,
//         KeeperData.Core.Storage.Dtos.StorageObjectInfo objectInfo,
//         SanitizedExportOptions options,
//         CancellationToken cancellationToken)
//     {
//         using var sourceStream = await OpenPreparedSourceStreamAsync(sourceStorage, objectInfo, options, cancellationToken);
//         if (sourceStream.IsZipArchive)
//         {
//             return StreamingFileAnalysis.ForZipArchive();
//         }
//
//         using var parser = CreateParser(sourceStream.Reader!, sourceStream.Delimiter);
//         if (parser.EndOfData)
//         {
//             throw new InvalidDataException($"Source object '{objectInfo.Key}' is empty.");
//         }
//
//         var headers = parser.ReadFields() ?? throw new InvalidDataException($"Source object '{objectInfo.Key}' does not contain a header row.");
//         var exportRows = new Queue<string[]>(options.ExportRecordCount);
//         var sortAnalyzer = new SortAnalyzer(headers);
//         long rowCount = 0;
//         var invalidRowCount = 0;
//
//         while (!parser.EndOfData)
//         {
//             var row = parser.ReadFields() ?? [];
//             rowCount++;
//
//             if (row.Length != headers.Length)
//             {
//                 invalidRowCount++;
//             }
//
//             sortAnalyzer.Observe(row);
//
//             if (exportRows.Count == options.ExportRecordCount)
//             {
//                 exportRows.Dequeue();
//             }
//
//             exportRows.Enqueue(PadRow(row, headers.Length));
//         }
//
//         return new StreamingFileAnalysis(headers, rowCount, invalidRowCount, [.. exportRows], sortAnalyzer.Summarise(), sourceStream.Delimiter, false);
//     }
//
//     private async Task<PreparedSourceSession> OpenPreparedSourceStreamAsync(
//         IBlobStorageServiceReadOnly sourceStorage,
//         KeeperData.Core.Storage.Dtos.StorageObjectInfo objectInfo,
//         SanitizedExportOptions options,
//         CancellationToken cancellationToken)
//     {
//         var sourceStream = await sourceStorage.OpenReadAsync(objectInfo.Key, cancellationToken);
//         Stream preparedStream = sourceStream;
//
//         if (options.SourceFilesEncrypted)
//         {
//             var credentials = _passwordSaltService.Get(objectInfo.Key);
//             preparedStream = WrapDecryptStream(preparedStream, credentials.Password, credentials.Salt);
//         }
//
//         var extension = Path.GetExtension(objectInfo.Key).ToLowerInvariant();
//         if (extension == ".gz")
//         {
//             preparedStream = new GZipStream(preparedStream, CompressionMode.Decompress, leaveOpen: false);
//         }
//
//         if (extension == ".zip")
//         {
//             return new PreparedSourceSession(preparedStream, null, '\0', true);
//         }
//
//         var (compositeStream, firstLine) = await PeekFirstLineAsync(preparedStream, cancellationToken);
//         var delimiter = InferDelimiter(firstLine);
//         var reader = new StreamReader(compositeStream, Encoding.UTF8, detectEncodingFromByteOrderMarks: true, leaveOpen: false);
//         return new PreparedSourceSession(compositeStream, reader, delimiter, false);
//     }
//
//     private static async Task<(Stream CompositeStream, string FirstLine)> PeekFirstLineAsync(Stream stream, CancellationToken cancellationToken)
//     {
//         var buffer = new MemoryStream();
//         while (buffer.Length < 64 * 1024)
//         {
//             var nextByte = new byte[1];
//             var read = await stream.ReadAsync(nextByte, cancellationToken);
//             if (read == 0)
//             {
//                 break;
//             }
//
//             buffer.WriteByte(nextByte[0]);
//             if (nextByte[0] == '\n')
//             {
//                 break;
//             }
//         }
//
//         var firstBytes = buffer.ToArray();
//         var firstLine = Encoding.UTF8.GetString(firstBytes).TrimEnd('\r', '\n');
//         var compositeStream = new ConcatenatedStream(new MemoryStream(firstBytes, writable: false), stream);
//         return (compositeStream, firstLine);
//     }
//
//     private static char InferDelimiter(string firstLine)
//     {
//         var candidates = new[] { '|', ',', '\t', ';' };
//         return candidates
//             .Select(delimiter => new { Delimiter = delimiter, Score = firstLine.Count(character => character == delimiter) })
//             .OrderByDescending(item => item.Score)
//             .First().Delimiter;
//     }
//
//     private Stream WrapDecryptStream(Stream encryptedStream, string password, string salt)
//     {
//         var saltBytes = string.IsNullOrEmpty(salt) ? [] : Encoding.UTF8.GetBytes(salt);
//         var actualSalt = saltBytes.Length switch
//         {
//             0 => new byte[8],
//             < 8 => [.. saltBytes, .. new byte[8 - saltBytes.Length]],
//             _ => saltBytes
//         };
//
//         var pbkdf2 = new Rfc2898DeriveBytes(password, actualSalt, 32, HashAlgorithmName.SHA1);
//         var key = pbkdf2.GetBytes(32);
//         var aes = Aes.Create();
//         aes.Key = key;
//         aes.Mode = CipherMode.ECB;
//         aes.Padding = PaddingMode.PKCS7;
//         var decryptor = aes.CreateDecryptor();
//         return new CryptoStream(encryptedStream, decryptor, CryptoStreamMode.Read, leaveOpen: false);
//     }
//
//     private async Task StreamReferenceCsvToTargetAsync(
//         IBlobStorageServiceReadOnly sourceStorage,
//         IBlobStorageService targetStorage,
//         KeeperData.Core.Storage.Dtos.StorageObjectInfo objectInfo,
//         char delimiter,
//         string targetKey,
//         SanitizedExportOptions options,
//         CancellationToken cancellationToken)
//     {
//         using var sourceStream = await OpenPreparedSourceStreamAsync(sourceStorage, objectInfo, options, cancellationToken);
//         if (sourceStream.IsZipArchive)
//         {
//             throw new InvalidOperationException("Zip archives are not supported for reference streaming.");
//         }
//
//         using var parser = CreateParser(sourceStream.Reader!, delimiter);
//         await using var targetStream = await targetStorage.OpenWriteAsync(targetKey, "text/csv", cancellationToken: cancellationToken);
//         await using var writer = new StreamWriter(targetStream, new UTF8Encoding(false), 1024, leaveOpen: false);
//
//         while (!parser.EndOfData)
//         {
//             var row = parser.ReadFields() ?? [];
//             await writer.WriteLineAsync(string.Join(",", row.Select(EscapeCsvValue)));
//         }
//     }
//
//     private async Task UploadGeneratedCsvAsync(
//         IBlobStorageService targetStorage,
//         string targetKey,
//         IReadOnlyList<string> headers,
//         IReadOnlyCollection<string[]> rows,
//         CancellationToken cancellationToken)
//     {
//         await using var targetStream = await targetStorage.OpenWriteAsync(targetKey, "text/csv", cancellationToken: cancellationToken);
//         await using var writer = new StreamWriter(targetStream, new UTF8Encoding(false), 1024, leaveOpen: false);
//         await writer.WriteLineAsync(string.Join(",", headers.Select(EscapeCsvValue)));
//         foreach (var row in rows)
//         {
//             await writer.WriteLineAsync(string.Join(",", PadRow(row, headers.Count).Select(EscapeCsvValue)));
//         }
//     }
//
//     private async Task UploadAuditFilesAsync(
//         IBlobStorageService targetStorage,
//         IReadOnlyCollection<ExportManifestEntry> manifestEntries,
//         SanitizedExportOptions options,
//         string runId,
//         CancellationToken cancellationToken)
//     {
//         var manifestJson = JsonSerializer.Serialize(manifestEntries, new JsonSerializerOptions { WriteIndented = true });
//         await UploadTextAsync(targetStorage, CombineS3Key(options.TargetAuditPrefix, runId, $"export-manifest-{runId}.json"), manifestJson, cancellationToken);
//
//         var checksumLines = manifestEntries
//             .Where(entry => entry.Approved && !string.IsNullOrWhiteSpace(entry.ChecksumSha256) && !string.IsNullOrWhiteSpace(entry.OutputPath))
//             .Select(entry => $"{entry.ChecksumSha256}  {entry.OutputPath}");
//         await UploadTextAsync(targetStorage, CombineS3Key(options.TargetAuditPrefix, runId, $"checksums-{runId}.sha256"), string.Join('\n', checksumLines), cancellationToken);
//     }
//
//     private async Task UploadHeaderReportAsync(
//         IBlobStorageService targetStorage,
//         IReadOnlyCollection<HeaderComparisonReportEntry> headerReports,
//         SanitizedExportOptions options,
//         string runId,
//         CancellationToken cancellationToken)
//     {
//         using var writer = new StringWriter(CultureInfo.InvariantCulture);
//         writer.WriteLine("source_object_key,file_name,target_table_name,table_found,all_expected_columns_present,missing_columns,extra_columns,csv_header_count,table_column_count");
//         foreach (var report in headerReports.OrderBy(item => item.SourceObjectKey, StringComparer.OrdinalIgnoreCase))
//         {
//             writer.WriteLine(string.Join(",",
//                 EscapeCsvValue(report.SourceObjectKey),
//                 EscapeCsvValue(report.FileName),
//                 EscapeCsvValue(report.TargetTableName),
//                 EscapeCsvValue(report.TableFound.ToString()),
//                 EscapeCsvValue(report.AllExpectedColumnsPresent.ToString()),
//                 EscapeCsvValue(string.Join("|", report.MissingColumns)),
//                 EscapeCsvValue(string.Join("|", report.ExtraColumns)),
//                 EscapeCsvValue(report.CsvHeaderCount.ToString(CultureInfo.InvariantCulture)),
//                 EscapeCsvValue(report.TableColumnCount.ToString(CultureInfo.InvariantCulture))));
//         }
//
//         await UploadTextAsync(targetStorage, CombineS3Key(options.TargetAuditPrefix, runId, $"header-report-{runId}.csv"), writer.ToString(), cancellationToken);
//     }
//
//     private async Task UploadMappingReportAsync(
//         IBlobStorageService targetStorage,
//         ValueMappingStore valueMappingStore,
//         SanitizedExportOptions options,
//         string runId,
//         CancellationToken cancellationToken)
//     {
//         var mappingCsv = valueMappingStore.ToCsv();
//         await UploadTextAsync(targetStorage, CombineS3Key(options.TargetAuditPrefix, runId, $"anonymization-mapping-{runId}.csv"), mappingCsv, cancellationToken);
//     }
//
//     private async Task UploadTextAsync(IBlobStorageService targetStorage, string key, string content, CancellationToken cancellationToken) =>
//         await targetStorage.UploadAsync(key, Encoding.UTF8.GetBytes(content), "text/plain", cancellationToken: cancellationToken);
//
//     private async Task<string> ComputeSha256ForObjectAsync(IBlobStorageServiceReadOnly storage, string objectKey, CancellationToken cancellationToken)
//     {
//         await using var stream = await storage.OpenReadAsync(objectKey, cancellationToken);
//         using var sha256 = SHA256.Create();
//         var hash = await sha256.ComputeHashAsync(stream, cancellationToken);
//         return Convert.ToHexString(hash).ToLowerInvariant();
//     }
//
//     private async Task ImportApprovedObjectToPostgresAsync(
//         IBlobStorageServiceReadOnly targetStorage,
//         string targetObjectKey,
//         HeaderComparisonReportEntry headerReport,
//         SanitizedExportOptions options,
//         CancellationToken cancellationToken)
//     {
//         if (!options.ImportToPostgres)
//         {
//             _logger.LogDebug("Postgres import is disabled. Skipping import for {TargetObjectKey}.", targetObjectKey);
//             return;
//         }
//
//         if (!headerReport.TableFound)
//         {
//             _logger.LogWarning("Skipping Postgres import for {TargetObjectKey} because no target table definition was found.", targetObjectKey);
//             return;
//         }
//
//         if (string.IsNullOrWhiteSpace(options.PostgresConnectionString))
//         {
//             throw new InvalidOperationException("SANITISED_EXPORT_POSTGRES_CONNECTION_STRING is required when SANITISED_EXPORT_IMPORT_TO_POSTGRES is true.");
//         }
//
//         await using var stream = await targetStorage.OpenReadAsync(targetObjectKey, cancellationToken);
//         using var parser = CreateParser(new StreamReader(stream, Encoding.UTF8, true), ',');
//         if (parser.EndOfData)
//         {
//             _logger.LogWarning("Skipping Postgres import for {TargetObjectKey} because the exported CSV is empty.", targetObjectKey);
//             return;
//         }
//
//         var headers = parser.ReadFields() ?? [];
//         var allowedColumns = headers.Where(header => !headerReport.ExtraColumns.Contains(header, StringComparer.OrdinalIgnoreCase)).ToList();
//         if (allowedColumns.Count == 0)
//         {
//             _logger.LogWarning("Skipping Postgres import for {TargetObjectKey} because there are no matching table columns.", targetObjectKey);
//             return;
//         }
//
//         await using var connection = new NpgsqlConnection(options.PostgresConnectionString);
//         await connection.OpenAsync(cancellationToken);
//         _logger.LogInformation(
//             "Importing approved CSV from s3://{Bucket}/{Key} into Postgres table {Schema}.{Table} using {ColumnCount} column(s).",
//             options.TargetBucketName,
//             targetObjectKey,
//             options.PostgresSchema,
//             headerReport.TargetTableName,
//             allowedColumns.Count);
//         while (!parser.EndOfData)
//         {
//             var row = parser.ReadFields() ?? [];
//             await using var command = connection.CreateCommand();
//             command.CommandText = $"INSERT INTO {QuoteIdentifier(options.PostgresSchema)}.{QuoteIdentifier(headerReport.TargetTableName)} ({string.Join(", ", allowedColumns.Select(QuoteIdentifier))}) VALUES ({string.Join(", ", allowedColumns.Select((_, index) => $"@p{index}"))})";
//             for (var index = 0; index < allowedColumns.Count; index++)
//             {
//                 var valueIndex = Array.FindIndex(headers, header => string.Equals(header, allowedColumns[index], StringComparison.OrdinalIgnoreCase));
//                 var value = valueIndex >= 0 && valueIndex < row.Length ? row[valueIndex] : string.Empty;
//                 command.Parameters.AddWithValue($"p{index}", string.IsNullOrWhiteSpace(value) ? DBNull.Value : value);
//             }
//             await command.ExecuteNonQueryAsync(cancellationToken);
//         }
//         _logger.LogInformation("Completed Postgres import for s3://{Bucket}/{Key}.", options.TargetBucketName, targetObjectKey);
//     }
//
//     private static string CombineS3Key(params string[] parts) =>
//         string.Join("/", parts.Where(part => !string.IsNullOrWhiteSpace(part)).Select(part => part.Trim('/')));
//
//     private static string BuildS3Uri(string bucketName, string key) => $"s3://{bucketName}/{key}";
//
//     private static string[] PadRow(IReadOnlyList<string> row, int expectedLength)
//     {
//         if (row.Count == expectedLength)
//         {
//             return [.. row];
//         }
//
//         var result = new string[expectedLength];
//         for (var index = 0; index < expectedLength; index++)
//         {
//             result[index] = index < row.Count ? row[index] : string.Empty;
//         }
//         return result;
//     }
//
//     // Create a parser over an existing reader so streamed source and target objects can be handled without local files.
//     private static TextFieldParser CreateParser(TextReader reader, char delimiter)
//     {
//         var parser = new TextFieldParser(reader)
//         {
//             TextFieldType = FieldType.Delimited,
//             HasFieldsEnclosedInQuotes = true,
//             TrimWhiteSpace = false
//         };
//
//         parser.SetDelimiters(delimiter.ToString(CultureInfo.InvariantCulture));
//         return parser;
//     }
//
//     // Compare the CSV headers with the expected CTS table columns and keep the result for reporting and optional import.
//     private HeaderComparisonReportEntry CompareHeaders(
//         string targetTableName,
//         IReadOnlyList<string> csvHeaders,
//         IReadOnlyDictionary<string, CtsTableDefinition> tableDefinitions,
//         string sourceObjectKey,
//         string fileName)
//     {
//         if (!tableDefinitions.TryGetValue(targetTableName, out var tableDefinition))
//         {
//             _logger.LogWarning("No CTS table definition found for file {FileName} -> table {TargetTableName}.", fileName, targetTableName);
//             return new HeaderComparisonReportEntry(
//                 sourceObjectKey,
//                 fileName,
//                 targetTableName,
//                 false,
//                 false,
//                 [],
//                 [.. csvHeaders],
//                 csvHeaders.Count,
//                 0);
//         }
//
//         var csvHeaderSet = csvHeaders.ToHashSet(StringComparer.OrdinalIgnoreCase);
//         var tableColumnSet = tableDefinition.Columns.ToHashSet(StringComparer.OrdinalIgnoreCase);
//         var missingColumns = tableDefinition.Columns.Where(column => !csvHeaderSet.Contains(column)).ToList();
//         var extraColumns = csvHeaders.Where(column => !tableColumnSet.Contains(column)).ToList();
//
//         _logger.LogInformation(
//             "Header comparison for {FileName} against {TargetTableName}: MissingColumns={MissingCount}, ExtraColumns={ExtraCount}.",
//             fileName,
//             targetTableName,
//             missingColumns.Count,
//             extraColumns.Count);
//         if (missingColumns.Count > 0)
//         {
//             _logger.LogWarning("Missing columns for {FileName}: {MissingColumns}", fileName, string.Join(", ", missingColumns));
//         }
//         if (extraColumns.Count > 0)
//         {
//             _logger.LogWarning("Extra columns for {FileName}: {ExtraColumns}", fileName, string.Join(", ", extraColumns));
//         }
//
//         return new HeaderComparisonReportEntry(
//             sourceObjectKey,
//             fileName,
//             targetTableName,
//             true,
//             missingColumns.Count == 0,
//             missingColumns,
//             extraColumns,
//             csvHeaders.Count,
//             tableDefinition.Columns.Count);
//     }
//
//     // Map a timestamped file name like CT_PARAM_VALUE_GROUP_2026-02-22-074603.csv back to the target table name.
//     private static string GetTargetTableName(string fileName)
//     {
//         var stem = Path.GetFileNameWithoutExtension(fileName);
//         var match = Regex.Match(stem, @"^(?<table>CT_[A-Z0-9_]+?)_\d{4}-\d{2}-\d{2}-\d{6}$", RegexOptions.IgnoreCase);
//         return match.Success ? match.Groups["table"].Value.ToUpperInvariant() : stem.ToUpperInvariant();
//     }
//
//     private static string QuoteIdentifier(string identifier) =>
//         $"\"{identifier.Replace("\"", "\"\"", StringComparison.Ordinal)}\"";
//
//     private static string EscapeCsvValue(string value)
//     {
//         if (value.Contains('"', StringComparison.Ordinal))
//         {
//             value = value.Replace("\"", "\"\"", StringComparison.Ordinal);
//         }
//
//         return value.IndexOfAny([',', '"', '\r', '\n']) >= 0 ? $"\"{value}\"" : value;
//     }
//
//     // Keep output names deterministic but unique enough to avoid collisions across similar source files.
//     private static string BuildOutputName(string objectKey, string fileName)
//     {
//         var stem = Path.GetFileNameWithoutExtension(fileName);
//         return $"{stem}-{ShortHash($"{objectKey}|{fileName}")}.csv";
//     }
//
//     // Short hash is enough here because it is only used to keep generated object names stable and compact.
//     private static string ShortHash(string value) => ComputeSha256Text(value)[..12];
//
//     // Shared text hashing helper for file naming.
//     private static string ComputeSha256Text(string value)
//     {
//         var hash = SHA256.HashData(Encoding.UTF8.GetBytes(value));
//         return Convert.ToHexString(hash).ToLowerInvariant();
//     }
// }
//
// internal sealed class SanitizedExportOptions
// {
//     public string BucketName { get; set; } = string.Empty;
//     public string Prefix { get; set; } = string.Empty;
//     public string TargetBucketName { get; set; } = string.Empty;
//     public string TargetExportPrefix { get; set; } = "exports";
//     public string TargetRejectedPrefix { get; set; } = "rejected";
//     public string TargetAuditPrefix { get; set; } = "audit";
//     public string Region { get; set; } = "eu-west-2";
//     public string? ServiceUrl { get; set; }
//     public string? AccessKey { get; set; }
//     public string? SecretKey { get; set; }
//     public int SampleSize { get; set; } = 20_000;
//     public int ExportRecordCount { get; set; } = 1_000;
//     public bool SourceFilesEncrypted { get; set; } = true;
//     public string CtsTablesSqlPath { get; set; } = "/DEFRA/CADS/cts_tables.sql";
//     public bool GenerateHeaderReport { get; set; } = true;
//     public bool ImportToPostgres { get; set; }
//     public string? PostgresConnectionString { get; set; }
//     public string PostgresSchema { get; set; } = "public";
//     public List<string> ReferenceTablePatterns { get; set; } = [];
//     public List<string> ApprovedOutputPatterns { get; set; } = [];
//     public List<FieldMappingRule> FieldMappings { get; set; } = [];
//
//     // Fill in the sensible defaults so the command still works with a minimal config.
//     public void Normalise()
//     {
//         ReferenceTablePatterns = [.. (ReferenceTablePatterns.Count == 0 ? DefaultReferencePatterns : ReferenceTablePatterns)
//             .Where(static value => !string.IsNullOrWhiteSpace(value))
//             .Select(static value => value.Trim())];
//
//         ApprovedOutputPatterns = [.. (ApprovedOutputPatterns.Count == 0 ? ["*.csv"] : ApprovedOutputPatterns)
//             .Where(static value => !string.IsNullOrWhiteSpace(value))
//             .Select(static value => value.Trim())];
//
//         if (ExportRecordCount <= 0)
//         {
//             ExportRecordCount = SampleSize;
//         }
//
//         FieldMappings = [.. (FieldMappings.Count == 0 ? DefaultFieldMappings : FieldMappings)
//             .Where(static mapping => !string.IsNullOrWhiteSpace(mapping.ColumnName) && !string.IsNullOrWhiteSpace(mapping.DataType))
//             .Select(static mapping => mapping.Normalise())];
//     }
//
//     // Fail early on missing configuration rather than starting a partial export run.
//     public void Validate()
//     {
//         if (string.IsNullOrWhiteSpace(BucketName))
//         {
//             throw new InvalidOperationException("SANITISED_EXPORT_BUCKET_NAME is required.");
//         }
//
//         if (string.IsNullOrWhiteSpace(TargetBucketName))
//         {
//             throw new InvalidOperationException("SANITISED_EXPORT_TARGET_BUCKET_NAME is required.");
//         }
//
//         if (SampleSize <= 0)
//         {
//             throw new InvalidOperationException("SANITISED_EXPORT_SAMPLE_SIZE must be greater than zero.");
//         }
//
//         if (ExportRecordCount <= 0)
//         {
//             throw new InvalidOperationException("SANITISED_EXPORT_EXPORT_RECORD_COUNT must be greater than zero.");
//         }
//
//         if (string.IsNullOrWhiteSpace(CtsTablesSqlPath))
//         {
//             throw new InvalidOperationException("SANITISED_EXPORT_CTS_TABLES_SQL_PATH is required.");
//         }
//
//         if (ImportToPostgres && string.IsNullOrWhiteSpace(PostgresConnectionString))
//         {
//             throw new InvalidOperationException("SANITISED_EXPORT_POSTGRES_CONNECTION_STRING is required when SANITISED_EXPORT_IMPORT_TO_POSTGRES is true.");
//         }
//     }
//
//     private static readonly string[] DefaultReferencePatterns =
//     [
//         "countries",
//         "species",
//         "roles",
//         "premisestypes",
//         "premisesactivitytypes",
//         "siteidentifiertypes",
//         "productionusages",
//         "facilitybusinessactivitymaps",
//         "reference"
//     ];
//
//     private static readonly FieldMappingRule[] DefaultFieldMappings =
//     [
//         new() { ColumnName = "PERSON_TITLE", DataType = "title" },
//         new() { ColumnName = "PAR_TITLE", DataType = "title" },
//         new() { ColumnName = "PERSON_GIVEN_NAME", DataType = "first_name" },
//         new() { ColumnName = "PERSON_GIVEN_NAME2", DataType = "first_name" },
//         new() { ColumnName = "PERSON_INITIALS", DataType = "initial" },
//         new() { ColumnName = "PERSON_FAMILY_NAME", DataType = "last_name" },
//         new() { ColumnName = "PAR_SURNAME", DataType = "last_name" },
//         new() { ColumnName = "ORGANISATION_NAME", DataType = "company_name" },
//         new() { ColumnName = "ADR_NAME", DataType = "company_name" },
//         new() { ColumnName = "INTERNET_EMAIL_ADDRESS", DataType = "email" },
//         new() { ColumnName = "PAR_EMAIL_ADDRESS", DataType = "email" },
//         new() { ColumnName = "MOBILE_NUMBER", DataType = "mobile_number" },
//         new() { ColumnName = "PAR_MOBILE_NUMBER", DataType = "mobile_number" },
//         new() { ColumnName = "LOC_MOBILE_NUMBER", DataType = "mobile_number" },
//         new() { ColumnName = "TELEPHONE_NUMBER", DataType = "telephone_number" },
//         new() { ColumnName = "PAR_TEL_NUMBER", DataType = "telephone_number" },
//         new() { ColumnName = "LOC_TEL_NUMBER", DataType = "telephone_number" },
//         new() { ColumnName = "STREET", DataType = "street_address" },
//         new() { ColumnName = "LOCALITY", DataType = "secondary_address" },
//         new() { ColumnName = "TOWN", DataType = "city" },
//         new() { ColumnName = "POSTCODE", DataType = "postcode" },
//         new() { ColumnName = "ADR_POST_CODE", DataType = "postcode" },
//         new() { ColumnName = "PAON_DESCRIPTION", DataType = "building_number" },
//         new() { ColumnName = "SAON_DESCRIPTION", DataType = "secondary_address" },
//         new() { ColumnName = "UDPRN", DataType = "udprn" },
//         new() { ColumnName = "OS_MAP_REFERENCE", DataType = "map_reference" },
//         new() { ColumnName = "LOC_MAP_REFERENCE", DataType = "map_reference" },
//         new() { ColumnName = "EASTING", DataType = "easting" },
//         new() { ColumnName = "NORTHING", DataType = "northing" },
//         new() { ColumnName = "ADR_ADDRESS_2", DataType = "street_address" },
//         new() { ColumnName = "ADR_ADDRESS_3", DataType = "street_address" },
//         new() { ColumnName = "ADR_ADDRESS_4", DataType = "street_address" },
//         new() { ColumnName = "ADR_ADDRESS_5", DataType = "street_address" }
//     ];
// }
//
// internal sealed class FieldMappingRule
// {
//     public string? FilePattern { get; set; }
//     public string ColumnName { get; set; } = string.Empty;
//     public string DataType { get; set; } = string.Empty;
//
//     // Keep rule matching case-insensitive and predictable.
//     public FieldMappingRule Normalise() =>
//         new()
//         {
//             FilePattern = string.IsNullOrWhiteSpace(FilePattern) ? null : FilePattern.Trim(),
//             ColumnName = ColumnName.Trim(),
//             DataType = DataType.Trim()
//         };
// }
//
// internal sealed record HeaderComparisonReportEntry(
//     string SourceObjectKey,
//     string FileName,
//     string TargetTableName,
//     bool TableFound,
//     bool AllExpectedColumnsPresent,
//     IReadOnlyList<string> MissingColumns,
//     IReadOnlyList<string> ExtraColumns,
//     int CsvHeaderCount,
//     int TableColumnCount);
//
// internal sealed record CtsTableDefinition(
//     string TableName,
//     IReadOnlyList<string> Columns);
//
// internal sealed record StreamingProcessedFileResult(
//     ExportManifestEntry Entry,
//     HeaderComparisonReportEntry HeaderReport,
//     string? TargetObjectKey);
//
// internal sealed record StreamingFileAnalysis(
//     string[] Headers,
//     long RowCount,
//     int InvalidRowCount,
//     IReadOnlyList<string[]> ExportRows,
//     string SortSummary,
//     char Delimiter,
//     bool IsZipArchive)
// {
//     public FileAnalysis ToFileAnalysis() => new(Headers, RowCount, InvalidRowCount, ExportRows, SortSummary);
//
//     public static StreamingFileAnalysis ForZipArchive() => new([], 0, 0, [], "Zip archive detected.", ',', true);
// }
//
// internal sealed class PreparedSourceSession(Stream baseStream, TextReader? reader, char delimiter, bool isZipArchive) : IDisposable
// {
//     public Stream BaseStream { get; } = baseStream;
//     public TextReader? Reader { get; } = reader;
//     public char Delimiter { get; } = delimiter;
//     public bool IsZipArchive { get; } = isZipArchive;
//
//     public void Dispose()
//     {
//         Reader?.Dispose();
//         BaseStream.Dispose();
//     }
// }
//
// internal sealed class ConcatenatedStream(Stream first, Stream second) : Stream
// {
//     private Stream _current = first;
//     private readonly Stream _first = first;
//     private readonly Stream _second = second;
//
//     public override bool CanRead => true;
//     public override bool CanSeek => false;
//     public override bool CanWrite => false;
//     public override long Length => throw new NotSupportedException();
//     public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
//     public override void Flush() { }
//     public override Task FlushAsync(CancellationToken cancellationToken) => Task.CompletedTask;
//     public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
//     public override void SetLength(long value) => throw new NotSupportedException();
//     public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
//
//     public override int Read(byte[] buffer, int offset, int count)
//     {
//         var read = _current.Read(buffer, offset, count);
//         if (read > 0 || ReferenceEquals(_current, _second))
//         {
//             return read;
//         }
//
//         _current = _second;
//         return _current.Read(buffer, offset, count);
//     }
//
//     public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
//     {
//         var read = await _current.ReadAsync(buffer, cancellationToken);
//         if (read > 0 || ReferenceEquals(_current, _second))
//         {
//             return read;
//         }
//
//         _current = _second;
//         return await _current.ReadAsync(buffer, cancellationToken);
//     }
//
//     protected override void Dispose(bool disposing)
//     {
//         if (disposing)
//         {
//             _first.Dispose();
//             _second.Dispose();
//         }
//
//         base.Dispose(disposing);
//     }
// }
//
// internal enum TableKind
// {
//     Reference,
//     NonReference
// }
//
// internal static class FileClassifier
// {
//     // Reference files are allowed through unchanged; everything else is treated as needing anonymisation.
//     public static TableKind Classify(string fileName, IReadOnlyCollection<string> referencePatterns)
//     {
//         var name = Path.GetFileNameWithoutExtension(fileName);
//         return referencePatterns.Any(pattern => name.Contains(pattern, StringComparison.OrdinalIgnoreCase))
//             ? TableKind.Reference
//             : TableKind.NonReference;
//     }
// }
//
// internal static class OutputPolicy
// {
//     // Approval is pattern-based so ops can lock exports down without changing code.
//     public static bool IsAllowed(string outputName, IReadOnlyCollection<string> approvedPatterns) =>
//         approvedPatterns.Any(pattern => MatchesPattern(outputName, pattern));
//
//     // Simple wildcard matching is enough for the allow-list rules used here.
//     private static bool MatchesPattern(string fileName, string pattern)
//     {
//         if (pattern == "*")
//         {
//             return true;
//         }
//
//         var regex = "^" + System.Text.RegularExpressions.Regex.Escape(pattern)
//             .Replace("\\*", ".*", StringComparison.Ordinal)
//             .Replace("\\?", ".", StringComparison.Ordinal) + "$";
//
//         return System.Text.RegularExpressions.Regex.IsMatch(fileName, regex, System.Text.RegularExpressions.RegexOptions.IgnoreCase);
//     }
// }
//
// internal sealed record FileAnalysis(
//     string[] Headers,
//     long RowCount,
//     int InvalidRowCount,
//     IReadOnlyList<string[]> LastRows,
//     string SortSummary);
//
// internal static class DelimitedFile
// {
//     private static readonly char[] CandidateDelimiters = ['|', ',', '\t', ';'];
//
//     // Infer the delimiter from the first row so we can handle the common inbound formats with minimal config.
//     public static char InferDelimiter(string path)
//     {
//         using var reader = new StreamReader(path);
//         var firstLine = reader.ReadLine() ?? string.Empty;
//
//         return CandidateDelimiters
//             .Select(delimiter => new { Delimiter = delimiter, Score = firstLine.Count(character => character == delimiter) })
//             .OrderByDescending(item => item.Score)
//             .First().Delimiter;
//     }
//
//     // Read the source file once to capture headers, row counts, malformed rows, and the trailing sample used for export.
//     public static FileAnalysis Analyse(string path, char delimiter, int sampleSize)
//     {
//         using var parser = CreateParser(path, delimiter);
//         if (parser.EndOfData)
//         {
//             throw new InvalidDataException($"Source file '{path}' is empty.");
//         }
//
//         var headers = parser.ReadFields() ?? throw new InvalidDataException($"Source file '{path}' does not contain a header row.");
//         var lastRows = new Queue<string[]>(sampleSize);
//         var sortAnalyzer = new SortAnalyzer(headers);
//         long rowCount = 0;
//         var invalidRowCount = 0;
//
//         while (!parser.EndOfData)
//         {
//             var row = parser.ReadFields() ?? [];
//             rowCount++;
//
//             if (row.Length != headers.Length)
//             {
//                 invalidRowCount++;
//             }
//
//             sortAnalyzer.Observe(row);
//
//             if (lastRows.Count == sampleSize)
//             {
//                 lastRows.Dequeue();
//             }
//
//             lastRows.Enqueue(Pad(row, headers.Length));
//         }
//
//         return new FileAnalysis(headers, rowCount, invalidRowCount, lastRows.ToList(), sortAnalyzer.Summarise());
//     }
//
//     // Reference files are copied straight through, just normalised to CSV output.
//     public static void CopyAsCsv(string sourcePath, string outputPath, char delimiter)
//     {
//         using var parser = CreateParser(sourcePath, delimiter);
//         using var writer = new StreamWriter(outputPath, false, new UTF8Encoding(false));
//
//         while (!parser.EndOfData)
//         {
//             var row = parser.ReadFields() ?? [];
//             WriteCsvRow(writer, row);
//         }
//     }
//
//     // Write the final staged CSV for anonymised outputs.
//     public static async Task WriteCsvAsync(string outputPath, IReadOnlyList<string> headers, IReadOnlyCollection<string[]> rows, CancellationToken cancellationToken)
//     {
//         await using var writer = new StreamWriter(outputPath, false, new UTF8Encoding(false));
//         await writer.WriteLineAsync(string.Join(",", headers.Select(EscapeCsv)));
//
//         foreach (var row in rows)
//         {
//             cancellationToken.ThrowIfCancellationRequested();
//             await writer.WriteLineAsync(string.Join(",", Pad(row, headers.Count).Select(EscapeCsv)));
//         }
//     }
//
//     // Re-read the produced CSV so the validator can compare it against the analysed source shape.
//     public static (string[] Headers, List<string[]> Rows, int InvalidRows) ReadCsv(string path)
//     {
//         using var parser = CreateParser(path, ',');
//         if (parser.EndOfData)
//         {
//             throw new InvalidDataException($"Output file '{path}' is empty.");
//         }
//
//         var headers = parser.ReadFields() ?? throw new InvalidDataException($"Output file '{path}' does not contain a header row.");
//         var rows = new List<string[]>();
//         var invalidRows = 0;
//
//         while (!parser.EndOfData)
//         {
//             var row = parser.ReadFields() ?? [];
//             if (row.Length != headers.Length)
//             {
//                 invalidRows++;
//             }
//
//             rows.Add(Pad(row, headers.Length));
//         }
//
//         return (headers, rows, invalidRows);
//     }
//
//     // Centralise parser setup so reads behave consistently across source and output checks.
//     private static TextFieldParser CreateParser(string path, char delimiter)
//     {
//         var parser = new TextFieldParser(path)
//         {
//             TextFieldType = FieldType.Delimited,
//             HasFieldsEnclosedInQuotes = true,
//             TrimWhiteSpace = false
//         };
//
//         parser.SetDelimiters(delimiter.ToString(CultureInfo.InvariantCulture));
//         return parser;
//     }
//
//     // Shared CSV row writer for the copy path.
//     private static void WriteCsvRow(TextWriter writer, IReadOnlyCollection<string> row) =>
//         writer.WriteLine(string.Join(",", row.Select(EscapeCsv)));
//
//     // Minimal CSV escaping so exported files can be opened in standard tooling without corruption.
//     private static string EscapeCsv(string? value)
//     {
//         var text = value ?? string.Empty;
//         if (text.Contains('"', StringComparison.Ordinal))
//         {
//             text = text.Replace("\"", "\"\"", StringComparison.Ordinal);
//         }
//
//         return text.IndexOfAny([',', '"', '\r', '\n']) >= 0 ? $"\"{text}\"" : text;
//     }
//
//     // Ensure short or malformed rows still line up with the header count during analysis and validation.
//     private static string[] Pad(IReadOnlyList<string> row, int expectedLength)
//     {
//         if (row.Count == expectedLength)
//         {
//             return [.. row];
//         }
//
//         var result = new string[expectedLength];
//         for (var index = 0; index < expectedLength; index++)
//         {
//             result[index] = index < row.Count ? row[index] : string.Empty;
//         }
//
//         return result;
//     }
// }
//
// internal sealed class SortAnalyzer
// {
//     private readonly string[] _headers;
//     private readonly List<int> _candidateIndexes;
//     private readonly Dictionary<int, int> _ascendingHits = [];
//     private readonly Dictionary<int, int> _descendingHits = [];
//     private readonly Dictionary<int, string?> _previousValues = [];
//
//     // Pick a few likely ordering columns so we can record a rough sort hint in the audit output.
//     public SortAnalyzer(string[] headers)
//     {
//         _headers = headers;
//         _candidateIndexes = headers
//             .Select((header, index) => new { header, index })
//             .Where(item => item.header.Contains("DATE", StringComparison.OrdinalIgnoreCase)
//                 || item.header.Contains("ID", StringComparison.OrdinalIgnoreCase)
//                 || item.header.Contains("BATCH", StringComparison.OrdinalIgnoreCase)
//                 || item.header.Contains("UPDATED", StringComparison.OrdinalIgnoreCase)
//                 || item.header.Contains("EFFECTIVE", StringComparison.OrdinalIgnoreCase))
//             .Select(item => item.index)
//             .Take(3)
//             .ToList();
//     }
//
//     // Compare the current row to the previous row for the candidate sort columns.
//     public void Observe(string[] row)
//     {
//         foreach (var index in _candidateIndexes.Where(index => index < row.Length))
//         {
//             var currentValue = row[index];
//             if (!_previousValues.TryGetValue(index, out var previousValue) || string.IsNullOrWhiteSpace(previousValue) || string.IsNullOrWhiteSpace(currentValue))
//             {
//                 _previousValues[index] = currentValue;
//                 continue;
//             }
//
//             var comparison = Compare(previousValue, currentValue);
//             if (comparison <= 0)
//             {
//                 _ascendingHits[index] = _ascendingHits.GetValueOrDefault(index) + 1;
//             }
//
//             if (comparison >= 0)
//             {
//                 _descendingHits[index] = _descendingHits.GetValueOrDefault(index) + 1;
//             }
//
//             _previousValues[index] = currentValue;
//         }
//     }
//
//     // Return a lightweight sort summary for operators rather than trying to fully prove file ordering.
//     public string Summarise()
//     {
//         if (_candidateIndexes.Count == 0)
//         {
//             return "No obvious sort column detected.";
//         }
//
//         foreach (var index in _candidateIndexes)
//         {
//             var ascending = _ascendingHits.GetValueOrDefault(index);
//             var descending = _descendingHits.GetValueOrDefault(index);
//             if (ascending == 0 && descending == 0)
//             {
//                 continue;
//             }
//
//             var direction = ascending >= descending ? "ascending" : "descending";
//             return $"Likely sorted by {_headers[index]} ({direction}).";
//         }
//
//         return "Sort order could not be inferred confidently.";
//     }
//
//     // Try date and number comparison first, then fall back to case-insensitive text comparison.
//     private static int Compare(string left, string right)
//     {
//         if (DateTime.TryParse(left, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var leftDate)
//             && DateTime.TryParse(right, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var rightDate))
//         {
//             return leftDate.CompareTo(rightDate);
//         }
//
//         if (decimal.TryParse(left, NumberStyles.Any, CultureInfo.InvariantCulture, out var leftDecimal)
//             && decimal.TryParse(right, NumberStyles.Any, CultureInfo.InvariantCulture, out var rightDecimal))
//         {
//             return leftDecimal.CompareTo(rightDecimal);
//         }
//
//         return string.Compare(left, right, StringComparison.OrdinalIgnoreCase);
//     }
// }
//
// internal sealed class ValidationResult
// {
//     // No messages means the file passed all checks.
//     public bool IsApproved => Messages.Count == 0;
//     public List<string> Messages { get; } = [];
// }
//
// internal static class OutputValidator
// {
//     private static readonly HashSet<string> SensitiveHeaders =
//     [
//         "PERSON_TITLE",
//         "PERSON_GIVEN_NAME",
//         "PERSON_GIVEN_NAME2",
//         "PERSON_INITIALS",
//         "PERSON_FAMILY_NAME",
//         "ORGANISATION_NAME",
//         "INTERNET_EMAIL_ADDRESS",
//         "MOBILE_NUMBER",
//         "TELEPHONE_NUMBER",
//         "STREET",
//         "LOCALITY",
//         "TOWN",
//         "POSTCODE",
//         "PAON_DESCRIPTION",
//         "SAON_DESCRIPTION",
//         "UDPRN",
//         "OS_MAP_REFERENCE",
//         "EASTING",
//         "NORTHING",
//         "PAR_TITLE",
//         "PAR_INITIALS",
//         "PAR_SURNAME",
//         "PAR_TEL_NUMBER",
//         "PAR_MOBILE_NUMBER",
//         "PAR_EMAIL_ADDRESS",
//         "ADR_NAME",
//         "ADR_ADDRESS_2",
//         "ADR_ADDRESS_3",
//         "ADR_ADDRESS_4",
//         "ADR_ADDRESS_5",
//         "ADR_POST_CODE",
//         "LOC_TEL_NUMBER",
//         "LOC_MOBILE_NUMBER",
//         "LOC_MAP_REFERENCE"
//     ];
//
//     // Sanity-check the staged CSV before it is allowed into the controlled export area.
//     public static ValidationResult Validate(
//         string sourcePath,
//         string outputPath,
//         TableKind tableKind,
//         FileAnalysis sourceAnalysis,
//         int sampleSize)
//     {
//         var result = new ValidationResult();
//         var output = DelimitedFile.ReadCsv(outputPath);
//
//         if (!sourceAnalysis.Headers.SequenceEqual(output.Headers, StringComparer.Ordinal))
//         {
//             result.Messages.Add("Output header does not match source header order.");
//         }
//
//         if (output.InvalidRows > 0)
//         {
//             result.Messages.Add("Output contains malformed rows.");
//         }
//
//         if (sourceAnalysis.InvalidRowCount > 0)
//         {
//             result.Messages.Add("Source contains malformed rows, manual review required.");
//         }
//
//         var expectedRows = tableKind == TableKind.Reference
//             ? sourceAnalysis.RowCount
//             : Math.Min(sourceAnalysis.RowCount, sampleSize);
//
//         if (output.Rows.Count != expectedRows)
//         {
//             result.Messages.Add($"Output row count {output.Rows.Count} does not match expected {expectedRows}.");
//         }
//
//         if (tableKind == TableKind.NonReference)
//         {
//             ValidateAnonymisation(sourceAnalysis, output, result);
//         }
//
//         if (tableKind == TableKind.Reference && sourceAnalysis.RowCount == 0)
//         {
//             result.Messages.Add("Reference table is empty.");
//         }
//
//         return result;
//     }
//
//     // Streaming mode validates the generated rows in memory before the object is uploaded to its final S3 prefix.
//     public static ValidationResult ValidateGenerated(
//         IReadOnlyList<string> headers,
//         IReadOnlyList<string[]> outputRows,
//         TableKind tableKind,
//         FileAnalysis sourceAnalysis,
//         int expectedOutputRows)
//     {
//         var result = new ValidationResult();
//
//         if (!sourceAnalysis.Headers.SequenceEqual(headers, StringComparer.Ordinal))
//         {
//             result.Messages.Add("Output header does not match source header order.");
//         }
//
//         if (outputRows.Any(row => row.Length != headers.Count))
//         {
//             result.Messages.Add("Output contains malformed rows.");
//         }
//
//         if (sourceAnalysis.InvalidRowCount > 0)
//         {
//             result.Messages.Add("Source contains malformed rows, manual review required.");
//         }
//
//         var expectedRows = tableKind == TableKind.Reference
//             ? sourceAnalysis.RowCount
//             : Math.Min(sourceAnalysis.RowCount, expectedOutputRows);
//
//         if (outputRows.Count != expectedRows)
//         {
//             result.Messages.Add($"Output row count {outputRows.Count} does not match expected {expectedRows}.");
//         }
//
//         if (tableKind == TableKind.NonReference)
//         {
//             ValidateGeneratedAnonymisation(sourceAnalysis, headers, outputRows, result);
//         }
//
//         if (tableKind == TableKind.Reference && sourceAnalysis.RowCount == 0)
//         {
//             result.Messages.Add("Reference table is empty.");
//         }
//
//         return result;
//     }
//
//     // For anonymised outputs, make sure at least one sensitive field actually changed in the sampled rows.
//     private static void ValidateAnonymisation(FileAnalysis sourceAnalysis, (string[] Headers, List<string[]> Rows, int InvalidRows) output, ValidationResult result)
//     {
//         var sensitiveIndexes = sourceAnalysis.Headers
//             .Select((header, index) => new { header, index })
//             .Where(item => SensitiveHeaders.Contains(item.header))
//             .Select(item => item.index)
//             .ToList();
//
//         if (sensitiveIndexes.Count == 0)
//         {
//             return;
//         }
//
//         var anyChanged = false;
//         for (var rowIndex = 0; rowIndex < Math.Min(sourceAnalysis.LastRows.Count, output.Rows.Count); rowIndex++)
//         {
//             foreach (var columnIndex in sensitiveIndexes)
//             {
//                 var sourceValue = sourceAnalysis.LastRows[rowIndex][columnIndex];
//                 var outputValue = output.Rows[rowIndex][columnIndex];
//
//                 if (!string.IsNullOrWhiteSpace(sourceValue) && !string.Equals(sourceValue, outputValue, StringComparison.Ordinal))
//                 {
//                     anyChanged = true;
//                     break;
//                 }
//             }
//
//             if (anyChanged)
//             {
//                 break;
//             }
//         }
//
//         if (!anyChanged)
//         {
//             result.Messages.Add("No sensitive fields were changed in the non-reference sample.");
//         }
//     }
//
//     private static void ValidateGeneratedAnonymisation(
//         FileAnalysis sourceAnalysis,
//         IReadOnlyList<string> headers,
//         IReadOnlyList<string[]> outputRows,
//         ValidationResult result)
//     {
//         var sensitiveIndexes = headers
//             .Select((header, index) => new { header, index })
//             .Where(item => SensitiveHeaders.Contains(item.header))
//             .Select(item => item.index)
//             .ToList();
//
//         if (sensitiveIndexes.Count == 0)
//         {
//             return;
//         }
//
//         var anyChanged = false;
//         for (var rowIndex = 0; rowIndex < Math.Min(sourceAnalysis.LastRows.Count, outputRows.Count); rowIndex++)
//         {
//             foreach (var columnIndex in sensitiveIndexes)
//             {
//                 var sourceValue = sourceAnalysis.LastRows[rowIndex][columnIndex];
//                 var outputValue = columnIndex < outputRows[rowIndex].Length ? outputRows[rowIndex][columnIndex] : string.Empty;
//
//                 if (!string.IsNullOrWhiteSpace(sourceValue) && !string.Equals(sourceValue, outputValue, StringComparison.Ordinal))
//                 {
//                     anyChanged = true;
//                     break;
//                 }
//             }
//
//             if (anyChanged)
//             {
//                 break;
//             }
//         }
//
//         if (!anyChanged)
//         {
//             result.Messages.Add("No sensitive fields were changed in the non-reference sample.");
//         }
//     }
// }
//
// internal static class RowAnonymizer
// {
//     private const string MobileNumberFormat = "07#########";
//     private const string TelephoneNumberFormat = "01### ######";
//     private const string PostcodeFormat = "??# #??";
//     private const string MapReferenceFormat = "??########";
//
//     private static readonly HashSet<string> PreserveHeaders =
//     [
//         "PARTY_ID",
//         "PAR_ID",
//         "CPH",
//         "LID_FULL_IDENTIFIER",
//         "FEATURE_NAME",
//         "CPH_TYPE",
//         "LTY_LOC_TYPE",
//         "ROLES",
//         "CPHS",
//         "SECONDARY_CPH",
//         "ANIMAL_SPECIES_CODE",
//         "ANIMAL_PRODUCTION_USAGE_CODE",
//         "HERDMARK",
//         "CPHH",
//         "BATCH_ID"
//     ];
//
//     // Replace selected sensitive columns with deterministic fake data while leaving identifiers needed for analysis intact.
//     public static string[] Anonymize(
//         string fileName,
//         IReadOnlyList<string> headers,
//         string[] row,
//         IReadOnlyCollection<FieldMappingRule> fieldMappings,
//         ValueMappingStore valueMappingStore)
//     {
//         var output = row.ToArray();
//         var values = headers
//             .Select((header, index) => new { header, value = index < row.Length ? row[index] : string.Empty })
//             .ToDictionary(item => item.header, item => item.value, StringComparer.Ordinal);
//
//         var seedSource = BuildSeedSource(fileName, values);
//         var faker = new Bogus.Faker("en_GB") { Random = new Bogus.Randomizer(GetStableSeed(seedSource)) };
//
//         for (var index = 0; index < headers.Count; index++)
//         {
//             var header = headers[index];
//             if (PreserveHeaders.Contains(header) || string.IsNullOrWhiteSpace(output[index]))
//             {
//                 continue;
//             }
//
//             var mapping = FindApplicableMapping(fileName, header, fieldMappings);
//             if (mapping == null)
//             {
//                 continue;
//             }
//
//             output[index] = valueMappingStore.GetOrAdd(
//                 mapping,
//                 header,
//                 output[index],
//                 () => GenerateFakeValue(mapping.DataType, faker));
//         }
//
//         return output;
//     }
//
//     // Match a rule by file pattern and column, with file-specific rules winning over global ones.
//     private static FieldMappingRule? FindApplicableMapping(
//         string fileName,
//         string header,
//         IReadOnlyCollection<FieldMappingRule> fieldMappings)
//     {
//         return fieldMappings
//             .Where(mapping => string.Equals(mapping.ColumnName, header, StringComparison.OrdinalIgnoreCase))
//             .OrderBy(mapping => string.IsNullOrWhiteSpace(mapping.FilePattern) ? 1 : 0)
//             .FirstOrDefault(mapping =>
//                 string.IsNullOrWhiteSpace(mapping.FilePattern)
//                 || fileName.Contains(mapping.FilePattern, StringComparison.OrdinalIgnoreCase));
//     }
//
//     // Keep faker generation in one place so the mapping store can call it consistently.
//     private static string GenerateFakeValue(string dataType, Faker faker) => dataType.ToLowerInvariant() switch
//     {
//         "title" => faker.Name.Prefix(),
//         "first_name" => faker.Name.FirstName(),
//         "initial" => faker.Name.FirstName()[..1],
//         "last_name" => faker.Name.LastName(),
//         "company_name" => faker.Company.CompanyName(),
//         "email" => faker.Internet.Email(),
//         "mobile_number" => faker.Phone.PhoneNumber(MobileNumberFormat),
//         "telephone_number" => faker.Phone.PhoneNumber(TelephoneNumberFormat),
//         "street_address" => faker.Address.StreetAddress(),
//         "secondary_address" => faker.Address.SecondaryAddress(),
//         "city" => faker.Address.City(),
//         "postcode" => faker.Address.ZipCode(PostcodeFormat),
//         "building_number" => faker.Address.BuildingNumber(),
//         "udprn" => faker.Random.Int(10_000_000, 99_999_999).ToString(CultureInfo.InvariantCulture),
//         "map_reference" => faker.Random.Replace(MapReferenceFormat).ToUpperInvariant(),
//         "easting" => faker.Random.Int(100000, 999999).ToString(CultureInfo.InvariantCulture),
//         "northing" => faker.Random.Int(200000, 999999).ToString(CultureInfo.InvariantCulture),
//         _ => throw new InvalidOperationException($"Unsupported anonymisation data type '{dataType}'.")
//     };
//
//     // Seed from a stable business identifier where possible so repeated runs stay consistent enough to compare.
//     private static string BuildSeedSource(string fileName, IReadOnlyDictionary<string, string> values)
//     {
//         foreach (var key in new[] { "PARTY_ID", "PAR_ID", "CPH", "LID_FULL_IDENTIFIER", "UDPRN" })
//         {
//             if (values.TryGetValue(key, out var value) && !string.IsNullOrWhiteSpace(value))
//             {
//                 return $"{fileName}:{key}:{value}";
//             }
//         }
//
//         return $"{fileName}:{string.Join('|', values.OrderBy(item => item.Key, StringComparer.Ordinal).Select(item => item.Value))}";
//     }
//
//     // MD5 is fine here because we only need a quick stable seed, not a security boundary.
//     private static int GetStableSeed(string identifier)
//     {
//         var hash = MD5.HashData(Encoding.UTF8.GetBytes(identifier));
//         return BitConverter.ToInt32(hash, 0);
//     }
// }
//
// internal sealed class ValueMappingStore
// {
//     private readonly ILogger _logger;
//     private readonly Dictionary<string, string> _mappings = new(StringComparer.Ordinal);
//     public int ReusedValues { get; private set; }
//     public int NewValues { get; private set; }
//     public int TotalEntries => _mappings.Count;
//
//     private ValueMappingStore(ILogger logger)
//     {
//         _logger = logger;
//     }
//
//     public static ValueMappingStore CreateInMemory(ILogger logger) => new(logger);
//
//     // Reuse an existing fake value where we have one, otherwise create and store a new one.
//     public string GetOrAdd(FieldMappingRule mapping, string columnName, string originalValue, Func<string> createValue)
//     {
//         var key = BuildKey(mapping.FilePattern, columnName, mapping.DataType, originalValue);
//         if (_mappings.TryGetValue(key, out var existingValue))
//         {
//             ReusedValues++;
//             return existingValue;
//         }
//
//         var fakeValue = createValue();
//         _mappings[key] = fakeValue;
//         NewValues++;
//         _logger.LogDebug(
//             "Created new mapping entry for FilePattern={FilePattern}, Column={Column}, DataType={DataType}, OriginalValue={OriginalValue}, FakeValue={FakeValue}.",
//             mapping.FilePattern ?? "*",
//             columnName,
//             mapping.DataType,
//             originalValue,
//             fakeValue);
//         return fakeValue;
//     }
//
//     // Use data type and original value as the consistency key, with file pattern only narrowing when explicitly configured.
//     private static string BuildKey(string? filePattern, string columnName, string dataType, string originalValue) =>
//         string.Join('\u001F',
//             string.IsNullOrWhiteSpace(filePattern) ? "*" : filePattern.Trim(),
//             columnName.Trim(),
//             dataType.Trim(),
//             originalValue);
//
//     private static string Escape(string value)
//     {
//         if (value.Contains('"', StringComparison.Ordinal))
//         {
//             value = value.Replace("\"", "\"\"", StringComparison.Ordinal);
//         }
//
//         return value.IndexOfAny([',', '"', '\r', '\n']) >= 0 ? $"\"{value}\"" : value;
//     }
//
//     public string ToCsv()
//     {
//         using var writer = new StringWriter(CultureInfo.InvariantCulture);
//         writer.WriteLine("file_pattern,column_name,data_type,original_value,fake_value");
//         foreach (var item in _mappings.OrderBy(item => item.Key, StringComparer.Ordinal))
//         {
//             var parts = item.Key.Split('\u001F');
//             writer.WriteLine(string.Join(",",
//                 Escape(parts[0]),
//                 Escape(parts[1]),
//                 Escape(parts[2]),
//                 Escape(parts[3]),
//                 Escape(item.Value)));
//         }
//
//         return writer.ToString();
//     }
// }
//
// internal static class CtsTableDefinitionLoader
// {
//     // Parse the CTS DDL file into a simple table-name-to-column-name map for header checking and import planning.
//     public static IReadOnlyDictionary<string, CtsTableDefinition> Load(string sqlPath, ILogger logger)
//     {
//         if (!File.Exists(sqlPath))
//         {
//             throw new FileNotFoundException($"CTS table SQL file not found: {sqlPath}");
//         }
//
//         var sql = File.ReadAllText(sqlPath);
//         var tableMatches = Regex.Matches(
//             sql,
//             @"CREATE\s+TABLE\s+(?<table>[A-Z0-9_]+)\s*\((?<body>.*?)\);",
//             RegexOptions.Singleline | RegexOptions.IgnoreCase);
//
//         var definitions = new Dictionary<string, CtsTableDefinition>(StringComparer.OrdinalIgnoreCase);
//         foreach (Match match in tableMatches)
//         {
//             var tableName = match.Groups["table"].Value.ToUpperInvariant();
//             var body = match.Groups["body"].Value;
//             var columns = body
//                 .Split('\n', StringSplitOptions.RemoveEmptyEntries)
//                 .Select(line => line.Trim())
//                 .Where(line => !line.StartsWith("--", StringComparison.Ordinal)
//                     && !line.StartsWith("PRIMARY KEY", StringComparison.OrdinalIgnoreCase)
//                     && !line.StartsWith("CONSTRAINT", StringComparison.OrdinalIgnoreCase)
//                     && !line.StartsWith("FOREIGN KEY", StringComparison.OrdinalIgnoreCase))
//                 .Select(line => line.TrimEnd(','))
//                 .Select(line => Regex.Match(line, @"^(?<column>[A-Z0-9_]+)\s+", RegexOptions.IgnoreCase))
//                 .Where(columnMatch => columnMatch.Success)
//                 .Select(columnMatch => columnMatch.Groups["column"].Value.ToUpperInvariant())
//                 .ToList();
//
//             definitions[tableName] = new CtsTableDefinition(tableName, columns);
//         }
//
//         logger.LogInformation("Loaded {TableCount} CTS table definition(s) from {SqlPath}.", definitions.Count, sqlPath);
//         return definitions;
//     }
// }
//
// internal sealed record ExportManifestEntry(
//     string SourceObjectKey,
//     string? InputPath,
//     string? OutputPath,
//     bool Approved,
//     string TableKind,
//     long SourceRowCount,
//     int SourceInvalidRowCount,
//     string SortSummary,
//     string? ChecksumSha256,
//     IReadOnlyList<string> ValidationMessages,
//     long SourceEncryptedLength,
//     string? SourceETag)
// {
//     public static ExportManifestEntry ApprovedEntry(
//         string sourceObjectKey,
//         string inputPath,
//         string outputPath,
//         TableKind tableKind,
//         FileAnalysis analysis,
//         string checksumSha256,
//         IReadOnlyList<string> validationMessages,
//         long sourceEncryptedLength,
//         string? sourceETag) =>
//         new(
//             sourceObjectKey,
//             inputPath,
//             outputPath,
//             true,
//             tableKind.ToString(),
//             analysis.RowCount,
//             analysis.InvalidRowCount,
//             analysis.SortSummary,
//             checksumSha256,
//             validationMessages,
//             sourceEncryptedLength,
//             sourceETag);
//
//     public static ExportManifestEntry Rejected(
//         string sourceObjectKey,
//         string inputPath,
//         string outputPath,
//         TableKind tableKind,
//         FileAnalysis analysis,
//         string reason) =>
//         new(
//             sourceObjectKey,
//             inputPath,
//             outputPath,
//             false,
//             tableKind.ToString(),
//             analysis.RowCount,
//             analysis.InvalidRowCount,
//             analysis.SortSummary,
//             null,
//             [reason],
//             0,
//             null);
//
//     public static ExportManifestEntry Failed(string sourceObjectKey, string reason) =>
//         new(sourceObjectKey, null, null, false, "Unknown", 0, 0, "Not analysed.", null, [reason], 0, null);
// }