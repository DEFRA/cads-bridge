# CADS

CADS is the standalone controlled anonymised data set export tool for the KRDS Bridge project.

It streams source files from vendor S3, decrypts them using the same AES convention used within KRDS Bridge project, handles gzip in-stream, classifies files as reference or non-reference, anonymises configured fields, validates the output, and uploads approved CSV exports plus audit files to the CADS/project S3 bucket.

## Run

```bash
dotnet run --project tools/CadsBridge -- run
```

To target one bulk timestamp:

```bash
dotnet run --project tools/CadsBridge -- run --data 2026-02-22-074603
```

## Main settings

CADS reads settings from `appsettings.json` and/or environment variables.

Required or commonly used environment variables:

- `SANITISED_EXPORT_BUCKET_NAME`
- `SANITISED_EXPORT_PREFIX`
- `SANITISED_EXPORT_TARGET_BUCKET_NAME`
- `SANITISED_EXPORT_TARGET_EXPORT_PREFIX`
- `SANITISED_EXPORT_TARGET_REJECTED_PREFIX`
- `SANITISED_EXPORT_TARGET_AUDIT_PREFIX`
- `SANITISED_EXPORT_REGION`
- `SANITISED_EXPORT_SERVICE_URL`
- `SANITISED_EXPORT_ACCESS_KEY`
- `SANITISED_EXPORT_SECRET_KEY`
- `SANITISED_EXPORT_SAMPLE_SIZE`
- `SANITISED_EXPORT_EXPORT_RECORD_COUNT`
- `SANITISED_EXPORT_SOURCE_FILES_ENCRYPTED`
- `SANITISED_EXPORT_REFERENCE_TABLE_PATTERNS`
- `SANITISED_EXPORT_APPROVED_OUTPUT_PATTERNS`
- `SANITISED_EXPORT_CTS_TABLES_SQL_PATH`
- `SANITISED_EXPORT_GENERATE_HEADER_REPORT`
- `SANITISED_EXPORT_IMPORT_TO_POSTGRES`
- `SANITISED_EXPORT_POSTGRES_CONNECTION_STRING`
- `SANITISED_EXPORT_POSTGRES_SCHEMA`
- `AesSalt`

Legacy `SANITIZED_EXPORT_*` names are still accepted for backwards compatibility.

## Field mappings

Anonymisation is driven by field-mapping rules from the `SanitisedExport` config section.

Each mapping can define:

- `FilePattern`: optional file-name match such as `CT_PARTIES`
- `ColumnName`: source column to anonymise
- `DataType`: faker type to generate

Example:

```json
{
  "SanitisedExport": {
    "FieldMappings": [
      {
        "FilePattern": "CT_PARTIES",
        "ColumnName": "PAR_EMAIL_ADDRESS",
        "DataType": "email"
      },
      {
        "ColumnName": "PERSON_GIVEN_NAME",
        "DataType": "first_name"
      }
    ]
  }
}
```

Supported `DataType` values:

- `title`
- `first_name`
- `initial`
- `last_name`
- `company_name`
- `email`
- `mobile_number`
- `telephone_number`
- `street_address`
- `secondary_address`
- `city`
- `postcode`
- `building_number`
- `udprn`
- `map_reference`
- `easting`
- `northing`

## Header report

CADS can compare each CSV file against the expected CTS table definition from `cts_tables.sql`.

The comparison:

- derives the target table name from the CSV file name
- checks CSV headers against the expected table columns
- records missing columns
- records extra columns
- uploads a header report CSV to the audit S3 prefix when `SANITISED_EXPORT_GENERATE_HEADER_REPORT=true`

The CTS table definition file path is controlled by:

- `SANITISED_EXPORT_CTS_TABLES_SQL_PATH`

## Postgres import

CADS can optionally import approved output CSV files into Postgres.

If Postgres import is not needed, leave it disabled and CADS will only write the exported files and audit artefacts to S3.

Enable it with:

- `SANITISED_EXPORT_IMPORT_TO_POSTGRES=true`

Required settings for import:

- `SANITISED_EXPORT_POSTGRES_CONNECTION_STRING`
- `SANITISED_EXPORT_POSTGRES_SCHEMA`

Import behaviour:

- only approved files are imported
- target table name is inferred from the CSV file name
- shared columns between the CSV and target table are inserted
- extra CSV columns are ignored during import
- header comparison still runs and is available in the report

## Faker export row count

The number of anonymised output rows is configurable with:

- `SANITISED_EXPORT_EXPORT_RECORD_COUNT`

This is separate from:

- `SANITISED_EXPORT_SAMPLE_SIZE`

`SANITISED_EXPORT_SAMPLE_SIZE` controls how much source data is analysed.

`SANITISED_EXPORT_EXPORT_RECORD_COUNT` controls how many faker rows are actually written to non-reference outputs.

## Mapping report behaviour

During anonymisation CADS keeps the original-to-fake mapping in memory so repeated values stay consistent across files in the same run.

At the end of the run it uploads an anonymisation mapping report to the audit S3 prefix.

This means mapping consistency is guaranteed within a single run, but not across separate runs.

## Outputs

CADS creates:

- approved CSV files under `SANITISED_EXPORT_TARGET_EXPORT_PREFIX`
- rejected outputs under `SANITISED_EXPORT_TARGET_REJECTED_PREFIX`
- an audit manifest JSON under `SANITISED_EXPORT_TARGET_AUDIT_PREFIX`
- a SHA-256 checksum file under `SANITISED_EXPORT_TARGET_AUDIT_PREFIX`
- a header report under `SANITISED_EXPORT_TARGET_AUDIT_PREFIX` when enabled
- an anonymisation mapping report under `SANITISED_EXPORT_TARGET_AUDIT_PREFIX`

For non-reference files, the anonymised CSV output is stored in the target S3 export folder, not on local disk.

## Notes

- reference files are copied through as CSV
- non-reference files are anonymised using the trailing rows defined by `SANITISED_EXPORT_EXPORT_RECORD_COUNT`
- source files can be treated as encrypted or plain using `SANITISED_EXPORT_SOURCE_FILES_ENCRYPTED`
- CADS does not write decrypted or staged files to local disk
- zip archives are currently rejected in zero-local-storage mode
- CADS uses the existing `KeeperData.Core` and `KeeperData.Infrastructure` projects rather than reimplementing storage or crypto logic
