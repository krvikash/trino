#!/usr/bin/env bash

set -euo pipefail

cd ${BASH_SOURCE%/*}

projectName=""
imageRepositories=()
projectVersion="1-SNAPSHOT"
tag=""
jdkVersion=19
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
    echo "        -t    Tag (defaults to project version)"
    echo "        -a    Platform types for multi-arch build"
    echo "        -P    Push images to remote repository (only used when -a is provided)"
}

while getopts "hp:r:v:t:j:a:P" opt; do
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
    j )
        jdkVersion="${OPTARG}"
        ;;
    t )
        tag="${OPTARG}"
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

case ${projectName} in
  "data-server")
    moduleName="galaxy-buffer-data-server"
    ;;
  "discovery-server")
    moduleName="galaxy-buffer-discovery-server"
    ;;
  *)
    echo "Unsupported projectName"
    printUsage
    exit 1;
    ;;
esac

if [[ "${tag}" == "" ]]; then
    tag="${projectVersion}"
fi

imageTags=()

for imageRepository in ${imageRepositories[@]}; do
    if [[ "${imageRepository}" != "" && "${imageRepository}" != */ ]]; then
        imageRepository="${imageRepository}/"
    fi
    imageTags+=("${imageRepository}trino-buffer-service/${projectName}:${tag}")
done

buildArguments=""

for imageTag in ${imageTags[@]}; do
    buildArguments="${buildArguments} --tag ${imageTag}"
done

if [[ "${archTypes}" == "" ]]; then
    buildArguments="${buildArguments} --tag trino-buffer-service/${projectName}:${tag}"
    docker build "../${moduleName}" \
        --build-arg "PROJECT_VERSION=${projectVersion}" \
        --build-arg "JDK_VERSION=${jdkVersion}" \
        -f "../${moduleName}/Dockerfile" ${buildArguments}
else
    if [[ ${pushImages} -eq 1 ]]; then
        buildArguments="${buildArguments} --push"
    else
        buildArguments="${buildArguments} -o type=docker"
    fi
    docker buildx build "../${moduleName}" \
        -f "../${moduleName}/Dockerfile" \
        --build-arg "PROJECT_VERSION=${projectVersion}" \
        --build-arg "JDK_VERSION=${jdkVersion}" \
        --platform "${archTypes}" ${buildArguments}
fi
