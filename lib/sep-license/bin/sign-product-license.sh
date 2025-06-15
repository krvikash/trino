#!/usr/bin/env bash

set -eu -o pipefail -o posix

function error () {
    echo "$@"
    exit 1
}

if [[ $# -ne 3 ]]
then
    error "usage: sign-product-license.sh <private key file path> <unsigned license file path> <signed license file path>"
fi

PEM_FILE="$1"
LICENSE_FILE="$2"
SIGNED_LICENSE_FILE="$3"

if [[ ! -r "${PEM_FILE}" ]]
then
    error "Couldn't read from ${PEM_FILE} - you probably need to run generate-product-keys.sh first"
fi

if [[ ! -r "${LICENSE_FILE}" ]]
then
    error "Unsigned license file path ${LICENSE_FILE} must be readable"
fi

INPUT_FILE_CONTAINS_SIGNATURE="$(jq 'has("base64Signature")' "${LICENSE_FILE}")"
if [[ "${INPUT_FILE_CONTAINS_SIGNATURE}" == "true" ]]
then
    error "Unsigned license file ${LICENSE_FILE} already contains a signature"
fi

BASE64_SIGNATURE="$(jq --compact-output --join-output --sort-keys '.features |= sort_by(.)' "${LICENSE_FILE}" | openssl dgst -sha512 -sign "${PEM_FILE}" | base64 | tr -d '\n')"
jq --sort-keys ". + {base64Signature: \"${BASE64_SIGNATURE}\"}" "${LICENSE_FILE}" > "${SIGNED_LICENSE_FILE}"
