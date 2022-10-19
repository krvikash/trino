#!/usr/bin/env bash

set -euo pipefail

cd ${BASH_SOURCE%/*}

projectName=""
imageRepository=""
projectVersion="1-SNAPSHOT"
archTypes=""

function printUsage(){
    echo "Usage:"
    echo "    ${0} [option]"
    echo "Options:"
    echo "        -h    Help"
    echo "        -p    Project to build (data-server|discovery-server)"
    echo "        -r    Docker repository"
    echo "        -v    Project version"
    echo "        -a    Platform types for multi-arch build"
}

while getopts "hp:r:v:a:" opt; do
    case ${opt} in
    h )
        printUsage;
        exit 0;
        ;;
    p )
        projectName="${OPTARG}"
        ;;
    r )
        imageRepository="${OPTARG}"
        ;;
    v )
        projectVersion="${OPTARG}"
        ;;
    a )
        archTypes="${OPTARG}"
        ;;
    esac
done

if [[ "${projectName}" == "" ]]; then
    echo "Project to build not set"
    printUsage
    exit 1;
fi

if [[ "${imageRepository}" != "" && "${imageRepository}" != */ ]]; then
    imageRepository="${imageRepository}/"
fi

imageTag="${imageRepository}trino-buffer-service/${projectName}:${projectVersion}"

if [[ "${archTypes}" == "" ]]; then
    docker build \
        --build-arg "PROJECT_VERSION=${projectVersion}" \
        -t "${imageTag}" \
        -f "../${projectName}/Dockerfile" "../${projectName}"
else
    docker buildx build "../${projectName}" \
        -f "../${projectName}/Dockerfile" \
        --build-arg "PROJECT_VERSION=${projectVersion}" \
        --tag "${imageTag}" \
        --platform "${archTypes}"
fi
