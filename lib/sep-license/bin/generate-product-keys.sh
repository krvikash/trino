#!/usr/bin/env bash

set -xeu -o pipefail -o posix

function error () {
    echo "$@"
    exit 1
}

if [[ $# -ne 1 ]]
then
    error "usage: generate-product-keys.sh <private key file path>"
fi

PEM_FILE="$1"
PUBLIC_DER_FILE="${BASH_SOURCE%/*}/../src/main/resources/com/starburstdata/presto/license/presto-license_public_key.der"

# run private key generation in a subshell with a secure umask
( umask 077 && openssl genrsa -out "${PEM_FILE}" 4096 )

openssl rsa -in "${PEM_FILE}" -pubout -outform DER -out "${PUBLIC_DER_FILE}"
