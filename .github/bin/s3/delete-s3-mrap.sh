#!/usr/bin/env bash
#
# delete-s3-mrap.sh
#   Deletes an S3 Multi-Region Access Point created by setup-empty-s3-mrap.sh.
#   Requires 'aws' and 'jq' commands and valid AWS credentials.
#   Does not delete the underlying S3 bucket.
#   Exits 0 even on failure to avoid breaking CI cleanup.
#   A separate cleanup script will remove orphaned MRAPs.

set -uo pipefail

S3_SCRIPTS_DIR="${BASH_SOURCE%/*}"

if [[ ! -f "${S3_SCRIPTS_DIR}/.mrap-arn" || ! -f "${S3_SCRIPTS_DIR}/.mrap-identifier" ]]; then
    echo "Missing file ${S3_SCRIPTS_DIR}/.mrap-arn or ${S3_SCRIPTS_DIR}/.mrap-identifier"
    rm -f "${S3_SCRIPTS_DIR}/.mrap-arn" "${S3_SCRIPTS_DIR}/.mrap-identifier" || true
    exit 0
fi

MRAP_NAME=$(cat "${S3_SCRIPTS_DIR}/.mrap-identifier")
MRAP_ARN=$(cat "${S3_SCRIPTS_DIR}/.mrap-arn")
AWS_ACCOUNT_ID=$(echo "$MRAP_ARN" | cut -d: -f5)


echo "Deleting AWS S3 Multi-Region Access Point ${MRAP_NAME}"

# All MRAP operations must target the us-west-2 region
DELETE_OUTPUT=$(aws s3control delete-multi-region-access-point \
  --region us-west-2 \
  --account-id "${AWS_ACCOUNT_ID}" \
  --details '{"Name":"'"${MRAP_NAME}"'"}')

REQUEST_TOKEN_ARN=$(echo "${DELETE_OUTPUT}" | jq -r '.RequestTokenARN')
if [ -z "${REQUEST_TOKEN_ARN}" ] || [ "${REQUEST_TOKEN_ARN}" = "null" ]; then
    echo "Failed to get request token for deleting Multi-Region Access Point ${MRAP_NAME}"
    rm -f "${S3_SCRIPTS_DIR}/.mrap-arn" "${S3_SCRIPTS_DIR}/.mrap-identifier"
    exit 0
fi

echo "Waiting for Multi-Region Access Point ${MRAP_NAME} to be deleted"

# Timeout after 10 minutes. This operation typically takes 1-2 minutes.
TIMEOUT=600
START_TIME=$(date +%s)
while [ $(($(date +%s) - START_TIME)) -lt "${TIMEOUT}" ]; do
    OPERATION_STATUS=$(aws s3control describe-multi-region-access-point-operation \
      --region us-west-2 \
      --account-id "${AWS_ACCOUNT_ID}" \
      --request-token-arn "${REQUEST_TOKEN_ARN}" \
      --query 'AsyncOperation.RequestStatus' \
      --output text 2>/dev/null)

    if [ "${OPERATION_STATUS}" = "SUCCEEDED" ]; then
        echo "Multi-Region Access Point ${MRAP_NAME} has been deleted"
        break
    elif [ "${OPERATION_STATUS}" = "FAILED" ]; then
        echo "Failed to delete Multi-Region Access Point ${MRAP_NAME}"
        break
    fi

    sleep 10
done

if [ $(($(date +%s) - START_TIME)) -ge "${TIMEOUT}" ]; then
    echo "Timed out waiting for Multi-Region Access Point ${MRAP_NAME} to be deleted"
fi

rm -f "${S3_SCRIPTS_DIR}/.mrap-arn" "${S3_SCRIPTS_DIR}/.mrap-identifier"

exit 0
