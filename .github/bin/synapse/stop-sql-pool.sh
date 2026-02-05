#!/usr/bin/env bash
#
# stop-sql-pool.sh
#   Stops an Azure Synapse SQL pool for connector tests
#   Requires 'az' command and expects user to be logged in
#   Environment variables:
#     SYNAPSE_RESOURCE_GROUP (required)
#     SYNAPSE_WORKSPACE (required)
#     SYNAPSE_SQL_POOL (required)
#

set -euo pipefail

# Check log in state
az account show --output none

az synapse sql pool delete \
    --name "$SYNAPSE_SQL_POOL" \
    --resource-group "$SYNAPSE_RESOURCE_GROUP" \
    --workspace-name "$SYNAPSE_WORKSPACE" \
    --no-wait --yes
