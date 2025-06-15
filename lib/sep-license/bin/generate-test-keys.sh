#!/usr/bin/env bash

set -xeu -o pipefail -o posix

PEM_FILE="${BASH_SOURCE%/*}/../src/test/resources/com/starburstdata/presto/license/test-keypair.pem"
PRIVATE_DER_FILE="${BASH_SOURCE%/*}/../src/test/resources/com/starburstdata/presto/license/test-private_key.der"
PUBLIC_DER_FILE="${BASH_SOURCE%/*}/../src/test/resources/com/starburstdata/presto/license/test-public_key.der"

openssl genrsa -out "${PEM_FILE}" 4096
openssl pkcs8 -topk8 -inform PEM -outform DER -in "${PEM_FILE}" -out "${PRIVATE_DER_FILE}" -nocrypt
openssl rsa -in "${PEM_FILE}" -pubout -outform DER -out "${PUBLIC_DER_FILE}"
