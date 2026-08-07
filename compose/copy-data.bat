aws s3 cp ./data/CTSM_UKV_PROD_BULK_######_CT_EARTAG_TYPES_2026-02-22-074603.csv s3://cads-bridge-external-bucket/CTSM_UKV_PROD_BULK_######_CT_EARTAG_TYPES_2026-02-22-074603.csv --profile localstack
aws s3 cp ./data/CTSM_UKV_PROD_BULK_######_CT_LOCATION_TYPES_2026-02-22-074603.csv s3://cads-bridge-external-bucket/CTSM_UKV_PROD_BULK_######_CT_LOCATION_TYPES_2026-02-22-074603.csv --profile localstack
aws s3 cp ./data/CTSM_UKV_PROD_BULK_######_CT_LOCATION_RELATIONSHIPS_2026-02-22-074604.csv s3://cads-bridge-external-bucket/CTSM_UKV_PROD_BULK_######_CT_LOCATION_RELATIONSHIPS_2026-02-22-074604.csv --profile localstack

aws s3 ls cads-external-bucket --profile localstack  
aws s3 ls cads-internal-bucket --profile localstack  