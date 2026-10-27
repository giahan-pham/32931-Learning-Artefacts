#!/usr/bin/env bash
# Runs the full integration test: starts Localstack, creates the bucket, runs the test, cleans up.
set -e
cd "$(dirname "$0")"

export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1
export AWS_REQUEST_CHECKSUM_CALCULATION=when_required AWS_RESPONSE_CHECKSUM_VALIDATION=when_required
export S3_ENDPOINT_URL=http://localhost:4566

docker compose up -d
sleep 5
# Create the bucket. Use the aws CLI if installed, otherwise boto3.
if command -v aws >/dev/null 2>&1; then
    aws --endpoint-url="$S3_ENDPOINT_URL" s3 mb s3://nyc-duration || true
else
    python -c "
import boto3
s3 = boto3.client('s3', endpoint_url='$S3_ENDPOINT_URL')
if 'nyc-duration' not in [b['Name'] for b in s3.list_buckets()['Buckets']]:
    s3.create_bucket(Bucket='nyc-duration')
print('bucket nyc-duration ready')
"
fi

# Don't stop on a test failure, so Localstack still gets shut down below.
set +e
python integration_test.py
ERROR_CODE=$?

docker compose down
exit $ERROR_CODE
