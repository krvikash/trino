## Synapse integration setup

This setup is required to create the infrastructure for running tests.
These steps are handled automatically in CI.

1.  Verify you're logged in to the `starburst-engineering` account in the
    Azure CLI tool:

    ```bash
    az account show || az login
    az account set --subscription starburst-engineering
    ```

2.  Use the [secrets script](https://github.com/starburstdata/sep-ci-dev-tools/tree/main/secrets)
    from `starburstdata/sep-ci-dev-tools` to get the `SYNAPSE` secrets from the
    `sep-ci/sep/starburst-enterprise` secrets document:

    ```bash
    $(${SEP_CI_DEV_TOOLS}/secrets/secrets export sep-ci/sep/starburst-enterprise | \
        grep "SYNAPSE")
    ```

3.  Start a SQL pool for your tests and save the name:

    ```bash
    export SYNAPSE_SQL_POOL=$(.github/bin/synapse/start-sql-pool.sh)
    ```
> [!NOTE]
> Set the env var `SYNAPSE_POOL_TTL` to a number of hours to keep pool running (default 2), e.g.
>
> ```bash
> export SYNAPSE_SQL_POOL=$(SYNAPSE_POOL_TTL=5 .github/bin/synapse/start-sql-pool.sh)
> ```

4.  Run tests, e.g.:

    ```bash
    ./mvnw clean test -pl plugin/starburst-synapse \
        -Dtest.synapse.jdbc.endpoint=${SYNAPSE_ENDPOINT:?} \
        -Dtest.synapse.jdbc.user=${SYNAPSE_USER:?} \
        -Dtest.synapse.jdbc.password=${SYNAPSE_PASSWORD:?} \
        -Dtest.synapse.jdbc.sqlpool=${SYNAPSE_SQL_POOL:?}
    ```

5.  Clean up your testing SQL pool:

    ```bash
    .github/bin/synapse/stop-sql-pool.sh
    unset SYNAPSE_SQL_POOL
    ```
