#!/usr/bin/env bash

set -eu -o pipefail -o posix

function error () {
    echo "$@"
    exit 1
}

PEM_FILE="${BASH_SOURCE%/*}/../src/test/resources/com/starburstdata/presto/license/test-keypair.pem"
TEST_LICENSE="${BASH_SOURCE%/*}/../src/test/resources/com/starburstdata/presto/license/test-license.json"
TEST_LICENSE_SORTED="${BASH_SOURCE%/*}/../src/test/resources/com/starburstdata/presto/license/test-license.json.sorted"
TEST_LICENSE_SIGNATURE="${BASH_SOURCE%/*}/../src/test/resources/com/starburstdata/presto/license/test-license.json.signature"
TEST_LICENSE_SIGNED="${BASH_SOURCE%/*}/../src/test/resources/com/starburstdata/presto/license/test-license.json.signed"

if [[ ! -r "${PEM_FILE}" ]]
then
    error "Couldn't read from ${PEM_FILE} - you probably need to run generate-test-keys.sh first"
fi

if [[ ! -r "${TEST_LICENSE}" ]]
then
    error "Couldn't read test license file: ${TEST_LICENSE}"
fi

jq --compact-output --join-output --sort-keys ".features |= sort_by(.)" "${TEST_LICENSE}" > "${TEST_LICENSE_SORTED}"
openssl dgst -sha512 -sign "${PEM_FILE}" -out "${TEST_LICENSE_SIGNATURE}" "${TEST_LICENSE_SORTED}"
jq --sort-keys ". + {base64Signature: \"$(base64 "${TEST_LICENSE_SIGNATURE}" | tr -d '\n')\"}" "${TEST_LICENSE}" > "${TEST_LICENSE_SIGNED}"
