#!/usr/bin/env bash

set -euo pipefail

cd ${BASH_SOURCE%/*}

projectName=""
imageRepositories=()
projectVersion="1-SNAPSHOT"
archTypes=""
pushImages=0

function printUsage(){
    echo "Usage:"
    echo "    ${0} [option]"
    echo "Options:"
    echo "        -h    Help"
    echo "        -p    Project to build (data-server|discovery-server)"
    echo "        -r    Docker repository"
    echo "        -v    Project version"
    echo "        -a    Platform types for multi-arch build"
    echo "        -P    Push images to remote repository (only used when -a is provided)"
}

while getopts "hp:r:v:a:P" opt; do
    case ${opt} in
    h )
        printUsage;
        exit 0;
        ;;
    p )
        projectName="${OPTARG}"
        ;;
    r )
        imageRepositories+=("${OPTARG}")
        ;;
    v )
        projectVersion="${OPTARG}"
        ;;
    a )
        archTypes="${OPTARG}"
        ;;
    P )
        pushImages=1
        ;;
    esac
done

if [[ "${projectName}" == "" ]]; then
    echo "Project to build not set"
    printUsage
    exit 1;
fi

imageTags=()

for imageRepository in ${imageRepositories[@]}; do
    if [[ "${imageRepository}" != "" && "${imageRepository}" != */ ]]; then
        imageRepository="${imageRepository}/"
    fi
    imageTags+=("${imageRepository}trino-buffer-service/${projectName}:${projectVersion}")
done

buildArguments=""

for imageTag in ${imageTags[@]}; do
    buildArguments="${buildArguments} --tag ${imageTag}"
done

if [[ "${archTypes}" == "" ]]; then
    buildArguments="${buildArguments} --tag trino-buffer-service/${projectName}:${projectVersion}"
    docker build "../${projectName}" \
        --build-arg "PROJECT_VERSION=${projectVersion}" \
        -f "../${projectName}/Dockerfile" ${buildArguments}
else
    if [[ ${pushImages} -eq 1 ]]; then
        buildArguments="${buildArguments} --push"
    else
        buildArguments="${buildArguments} -o type=docker"
    fi
    docker buildx build "../${projectName}" \
        -f "../${projectName}/Dockerfile" \
        --build-arg "PROJECT_VERSION=${projectVersion}" \
        --platform "${archTypes}" ${buildArguments}
fi
