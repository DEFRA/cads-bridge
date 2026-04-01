#!/bin/bash
sed -i 's/\r$//' "$0"

export AWS_REGION=eu-west-2
export AWS_DEFAULT_REGION=eu-west-2
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test

# S3 buckets
echo "Bootstrapping S3 setup..."

## Create External Bucket
EXTERNAL_BUCKET_NAME="cads-bridge-external-bucket"

existing_internal_bucket=$(awslocal s3api list-buckets \
  --query "Buckets[?Name=='$EXTERNAL_BUCKET_NAME'].Name" \
  --output text)

if [ "$existing_internal_bucket" == "$EXTERNAL_BUCKET_NAME" ]; then
  echo "S3 bucket already exists: $EXTERNAL_BUCKET_NAME"
else
  awslocal s3api create-bucket --bucket "$EXTERNAL_BUCKET_NAME" --region eu-west-2 \
    --create-bucket-configuration LocationConstraint=eu-west-2 \
    --endpoint-url=http://localhost:4566
  echo "S3 bucket created: $EXTERNAL_BUCKET_NAME"
fi

## Create Internal Bucket
INTERNAL_BUCKET_NAME="cads-bridge-internal-bucket"

existing_internal_bucket=$(awslocal s3api list-buckets \
  --query "Buckets[?Name=='$INTERNAL_BUCKET_NAME'].Name" \
  --output text)

if [ "$existing_internal_bucket" == "$INTERNAL_BUCKET_NAME" ]; then
  echo "S3 bucket already exists: $INTERNAL_BUCKET_NAME"
else
  awslocal s3api create-bucket --bucket "$INTERNAL_BUCKET_NAME" --region eu-west-2 \
    --create-bucket-configuration LocationConstraint=eu-west-2 \
    --endpoint-url=http://localhost:4566
  echo "S3 bucket created: $INTERNAL_BUCKET_NAME"
fi