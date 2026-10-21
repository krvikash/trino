#!/usr/bin/env bash
#
# Prepare a fresh "Deep Learning Base OSS Nvidia Driver GPU AMI (Ubuntu 26.04)" host for cork GPU development.
# Run on the host as the login user (sudo is used where needed). Idempotent: safe to re-run.
# Pass a GitHub token on stdin, to log in gh: `gh auth token | ssh <host> /tmp/setup-host.sh`.
# See dev/torin/README.md for creating the host and for the steps that must run locally first.

set -euo pipefail

JAVA_HOME_DIR=/usr/lib/jvm/temurin-25-jdk-amd64
CORK_DIR="${HOME}/cork"
TOOLS_DIR="${HOME}/bin/tools"

err() { printf 'error: %s\n' "$*" >&2; exit 1; }
step() { printf '\n=== %s\n' "$*"; }

# Read the token before anything else can consume stdin
[[ ! -t 0 ]] || err "pass a GitHub token on stdin: gh auth token | ssh <host> /tmp/setup-host.sh"
IFS= read -r GH_TOKEN || true
[[ -n "${GH_TOKEN}" ]] || err "no GitHub token on stdin"

# Prerequisites expected from the AMI and from the local steps
[[ "$(. /etc/os-release; echo "${VERSION_CODENAME}")" == resolute ]] || err "expected Ubuntu 26.04 (resolute)"
ldconfig -p | grep 'libnvrtc\.so\.13 ' >/dev/null || err "libnvrtc.so.13 not registered with ldconfig; is this the DLAMI?"
ldconfig -p | grep 'libnvJitLink\.so\.13 ' >/dev/null || err "libnvJitLink.so.13 not registered with ldconfig; is this the DLAMI?"
[[ -f "${HOME}/.m2/settings.xml" ]] || err "${HOME}/.m2/settings.xml missing; copy it from your local machine first"

step "Waiting for cloud-init"
cloud-init status --wait >/dev/null

step "Installing gh"
if ! command -v gh >/dev/null; then
    sudo install -d -m 0755 /etc/apt/keyrings
    curl -fsSL https://cli.github.com/packages/githubcli-archive-keyring.gpg | sudo tee /etc/apt/keyrings/githubcli.gpg >/dev/null
    echo "deb [signed-by=/etc/apt/keyrings/githubcli.gpg] https://cli.github.com/packages stable main" |
        sudo tee /etc/apt/sources.list.d/github-cli.list >/dev/null
    sudo apt-get update -q
    sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -q gh
fi
gh auth login --with-token <<<"${GH_TOKEN}"
gh auth setup-git

step "Installing Temurin 25"
if [[ ! -x "${JAVA_HOME_DIR}/bin/java" ]]; then
    sudo install -d -m 0755 /etc/apt/keyrings
    curl -fsSL https://packages.adoptium.net/artifactory/api/gpg/key/public | sudo gpg --dearmor --yes -o /etc/apt/keyrings/adoptium.gpg
    echo "deb [signed-by=/etc/apt/keyrings/adoptium.gpg] https://packages.adoptium.net/artifactory/deb resolute main" |
        sudo tee /etc/apt/sources.list.d/adoptium.list >/dev/null
    sudo apt-get update -q
    sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -q temurin-25-jdk
fi
"${JAVA_HOME_DIR}/bin/java" -version 2>&1 | head -1

step "Configuring shell environment"
# DLAMI's /etc/profile.d/dlami.sh already puts CUDA on PATH and LD_LIBRARY_PATH
# Must stay byte-identical to the marker already in ~/.profile on existing hosts, or a re-run appends a duplicate block
marker='# cork GPU dev (dev/gpu/setup-host.sh)'
if ! grep -qF "${marker}" "${HOME}/.profile"; then
    cat >>"${HOME}/.profile" <<EOF

${marker}
export JAVA_HOME=${JAVA_HOME_DIR}
export PATH="\${JAVA_HOME}/bin:\${HOME}/bin/tools:\${PATH}"
EOF
fi
export JAVA_HOME="${JAVA_HOME_DIR}"
export PATH="${JAVA_HOME}/bin:${TOOLS_DIR}:${PATH}"

step "Cloning tooltrash (workspace isolation tools)"
if [[ ! -d "${TOOLS_DIR}/.git" ]]; then
    git clone -q https://github.com/findepi/tooltrash "${TOOLS_DIR}"
fi
[[ -x "${TOOLS_DIR}/files/isolate-maven-workspace" ]] || err "isolate-maven-workspace not found in ${TOOLS_DIR}"

step "Cloning cork"
if [[ ! -d "${CORK_DIR}/.git" ]]; then
    git clone -q https://github.com/starburstdata/cork "${CORK_DIR}"
fi

step "Isolating the Maven workspace in ${CORK_DIR}"
if ! grep -qs 'enhancedLocalRepository.split' "${CORK_DIR}/.mvn/maven.config"; then
    (cd "${CORK_DIR}" && "${TOOLS_DIR}/files/isolate-maven-workspace")
fi

step "Building cork (downloads all dependencies)"
(cd "${CORK_DIR}" && ./mvnw -B -T1C install -nsu -DskipTests -Dmaven.javadoc.skip=true -Dmaven.source.skip=true -Dair.check.skip-all=true -Dskip.npm -Dskip.yarn)

step "Done"
