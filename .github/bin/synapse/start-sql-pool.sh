#!/usr/bin/env bash
#
# start-sql-pool.sh
#   Starts an Azure Synapse SQL pool for connector tests
#   Requires 'az' command and expects user to be logged in
#   Environment variables:
#     SYNAPSE_RESOURCE_GROUP (required)
#     SYNAPSE_WORKSPACE (required)
#     SYNAPSE_SQL_POOL (optional) if not specified, one will be generated
#     SYNAPSE_POOL_TTL (optional) minimum hours to keep pool up, default 2
#     SYNAPSE_POOL_TAGS (optional) JSON object containing additional tags
#           to apply to the pool
#

set -euo pipefail

# Check log in state
az account show --output none

ttl_seconds=$(( ${SYNAPSE_POOL_TTL:-2} * 3600 ))
now=$(date -u +"%s")
sql_pool_expiration=$(( now + ttl_seconds ))

pool_name="${SYNAPSE_SQL_POOL:-POOL_$(openssl rand -hex 4)}"

additional_tags=()
if [[ -n ${SYNAPSE_POOL_TAGS:-} ]]; then
    if command -v jq &> /dev/null; then
        while read -r tag; do
            additional_tags+=( "$tag" )
        done < <(echo "$SYNAPSE_POOL_TAGS" |
                    jq -r 'to_entries | .[] | "\(.key)=\(.value)"')
    else
        echo "jq is not installed; ignoring additional tags" >&2
    fi
fi

# Add expiration tag so the pool can be cleaned up later if the
# workflow fails before the cleanup job runs
az synapse sql pool create \
    --name "$pool_name" \
    --performance-level "DW300c" \
    --resource-group "$SYNAPSE_RESOURCE_GROUP" \
    --workspace-name "$SYNAPSE_WORKSPACE" \
    --tags "expiration=$sql_pool_expiration" "environment=test" "team=connectors-team" "${additional_tags[@]}"

echo "$pool_name"
