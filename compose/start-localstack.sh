#!/bin/bash
sed -i 's/\r$//' "$0"

export AWS_REGION=eu-west-2
export AWS_DEFAULT_REGION=eu-west-2
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test

set -e

# S3
echo "Creating S3 resources..."

## Create External Bucket
EXTERNAL_BUCKET_NAME="cads-external-bucket"

existing_internal_bucket=$(awslocal s3api list-buckets \
  --query "Buckets[?Name=='$EXTERNAL_BUCKET_NAME'].Name" \
  --output text)

if [ "$existing_internal_bucket" == "$EXTERNAL_BUCKET_NAME" ]; then
  echo "S3 bucket already exists: $EXTERNAL_BUCKET_NAME"
else
  awslocal s3api create-bucket \
    --bucket "$EXTERNAL_BUCKET_NAME" \
    --region eu-west-2 \
    --create-bucket-configuration LocationConstraint=eu-west-2 \
    --endpoint-url=http://localhost:4566
  echo "S3 bucket created: $EXTERNAL_BUCKET_NAME"
fi

## Create Internal Bucket
INTERNAL_BUCKET_NAME="cads-internal-bucket"

existing_internal_bucket=$(awslocal s3api list-buckets \
  --query "Buckets[?Name=='$INTERNAL_BUCKET_NAME'].Name" \
  --output text)

if [ "$existing_internal_bucket" == "$INTERNAL_BUCKET_NAME" ]; then
  echo "S3 bucket already exists: $INTERNAL_BUCKET_NAME"
else
  awslocal s3api create-bucket 
    --bucket "$INTERNAL_BUCKET_NAME" 
    --region eu-west-2 \
    --create-bucket-configuration LocationConstraint=eu-west-2 \
    --endpoint-url=http://localhost:4566
  echo "S3 bucket created: $INTERNAL_BUCKET_NAME"
fi