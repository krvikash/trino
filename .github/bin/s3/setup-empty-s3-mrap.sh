#!/usr/bin/env bash
#
# setup-empty-s3-mrap.sh
#   Creates an S3 Multi-Region Access Point targeting an existing bucket.
#   Requires 'aws' and 'jq' commands and valid AWS credentials.
#   The bucket must already exist (run setup-empty-s3-bucket.sh first).

set -euo pipefail

S3_SCRIPTS_DIR="${BASH_SOURCE%/*}"

if [[ ! -f "${S3_SCRIPTS_DIR}/.bucket-identifier" ]]; then
    echo "Missing file ${S3_SCRIPTS_DIR}/.bucket-identifier. Run setup-empty-s3-bucket.sh first."
    exit 1
fi

S3_BUCKET_IDENTIFIER=$(cat "${S3_SCRIPTS_DIR}/.bucket-identifier")
MRAP_NAME=trino-s3-mrap-ci-$(openssl rand -hex 8)
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query 'Account' --output text)

echo "Creating AWS S3 Multi-Region Access Point ${MRAP_NAME} targeting bucket ${S3_BUCKET_IDENTIFIER}"

# All MRAP operations must target the us-west-2 region
CREATE_OUTPUT=$(aws s3control create-multi-region-access-point \
  --region us-west-2 \
  --account-id "${AWS_ACCOUNT_ID}" \
  --details '{"Name":"'"${MRAP_NAME}"'","Regions":[{"Bucket":"'"${S3_BUCKET_IDENTIFIER}"'"}]}')

REQUEST_TOKEN_ARN=$(echo "${CREATE_OUTPUT}" | jq -r '.RequestTokenARN')
if [ -z "${REQUEST_TOKEN_ARN}" ] || [ "${REQUEST_TOKEN_ARN}" = "null" ]; then
    echo "Failed to create Multi-Region Access Point ${MRAP_NAME}"
    exit 1
fi

echo "${MRAP_NAME}" > "${S3_SCRIPTS_DIR}/.mrap-identifier"

echo "Waiting for Multi-Region Access Point ${MRAP_NAME} to be created"

# Timeout after 10 minutes. This operation typically takes 2-3 minutes.
TIMEOUT=600
START_TIME=$(date +%s)
while [ $(($(date +%s) - START_TIME)) -lt "${TIMEOUT}" ]; do
    OPERATION_STATUS=$(aws s3control describe-multi-region-access-point-operation \
      --region us-west-2 \
      --account-id "${AWS_ACCOUNT_ID}" \
      --request-token-arn "${REQUEST_TOKEN_ARN}" \
      --query 'AsyncOperation.RequestStatus' \
      --output text)

    if [ "${OPERATION_STATUS}" = "SUCCEEDED" ]; then
        echo "Multi-Region Access Point ${MRAP_NAME} has been created successfully"
        break
    elif [ "${OPERATION_STATUS}" = "FAILED" ]; then
        echo "Failed to create Multi-Region Access Point ${MRAP_NAME}"
        exit 1
    fi

    sleep 10
done

if [ $(($(date +%s) - START_TIME)) -ge "${TIMEOUT}" ]; then
    echo "Timed out waiting for Multi-Region Access Point ${MRAP_NAME} to be created"
    exit 1
fi

MRAP_DETAILS=$(aws s3control get-multi-region-access-point \
  --region us-west-2 \
  --account-id "${AWS_ACCOUNT_ID}" \
  --name "${MRAP_NAME}")

MRAP_ALIAS=$(echo "${MRAP_DETAILS}" | jq -r '.AccessPoint.Alias')
MRAP_ARN="arn:aws:s3::${AWS_ACCOUNT_ID}:accesspoint/${MRAP_ALIAS}"

echo "Multi-Region Access Point ARN: ${MRAP_ARN}"

echo "${MRAP_ARN}" > "${S3_SCRIPTS_DIR}/.mrap-arn"
