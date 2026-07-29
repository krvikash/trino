/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.snowflake;

import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import io.airlift.log.Level;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.Session;
import io.trino.plugin.jmx.JmxPlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.security.Identity;
import io.trino.testing.DistributedQueryRunner;
import io.trino.tpch.TpchTable;

import java.util.ArrayList;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static com.starburstdata.trino.plugin.snowflake.SnowflakeConnectorFlavour.PARALLEL;
import static com.starburstdata.trino.plugin.snowflake.SnowflakeServer.JDBC_URL;
import static com.starburstdata.trino.plugin.snowflake.SnowflakeServer.PASSWORD;
import static com.starburstdata.trino.plugin.snowflake.SnowflakeServer.ROLE;
import static com.starburstdata.trino.plugin.snowflake.SnowflakeServer.TEST_DATABASE;
import static com.starburstdata.trino.plugin.snowflake.SnowflakeServer.TEST_WAREHOUSE;
import static com.starburstdata.trino.plugin.snowflake.SnowflakeServer.USER;
import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.QueryAssertions.copyTpchTables;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Objects.requireNonNull;

public class SnowflakeQueryRunner
{
    public static final String TPCH_CATALOG = "tpch";

    public static final String SNOWFLAKE_CATALOG = "snowflake";

    public static final String TEST_SCHEMA = "test_schema_2";

    public static final String ALICE_USER = "alice";

    static {
        Logging logging = Logging.initialize();
        logging.setLevel("net.snowflake", Level.WARN);
    }

    public static Map<String, String> impersonationDisabled()
    {
        return ImmutableMap.of(
                "snowflake.role", ROLE);
    }

    public static Builder parallelBuilder()
    {
        return new Builder(createSessionForUser(USER))
                .withConnectorName(PARALLEL.getName());
    }

    public static Session createSessionForUser(String user)
    {
        return createSessionForUser(user, SNOWFLAKE_CATALOG);
    }

    public static Session createSessionForUser(String user, String catalogName)
    {
        return testSessionBuilder()
                .setCatalog(catalogName)
                .setSchema(TEST_SCHEMA)
                .setIdentity(Identity.forUser(user)
                        .build())
                .build();
    }

    public static class Builder
            extends DistributedQueryRunner.Builder<Builder>
    {
        private String connectorName;
        private Optional<String> warehouseName = Optional.of(TEST_WAREHOUSE);
        private Optional<String> databaseName = Optional.of(TEST_DATABASE);
        private Optional<String> privateKey = Optional.empty();
        private Optional<String> privateKeyPassphrase = Optional.empty();
        private Optional<String> password = Optional.of(PASSWORD);
        private String catalogName = SNOWFLAKE_CATALOG;
        private Optional<String> schemaName = Optional.empty();
        private final ImmutableMap.Builder<String, String> connectorProperties = ImmutableMap.builder();
        private int nodeCount = 3;
        private Iterable<TpchTable<?>> tpchTables = new ArrayList<>();

        protected Builder(Session defaultSession)
        {
            super(defaultSession);
        }

        @CanIgnoreReturnValue
        public Builder withConnectorName(String connectorName)
        {
            this.connectorName = requireNonNull(connectorName, "connectorName is null");
            return self();
        }

        @CanIgnoreReturnValue
        public Builder withWarehouse(Optional<String> warehouseName)
        {
            this.warehouseName = warehouseName;
            return self();
        }

        @CanIgnoreReturnValue
        public Builder withPrivateKey(Optional<String> privateKey)
        {
            this.privateKey = privateKey;
            return self();
        }

        @CanIgnoreReturnValue
        public Builder withPrivateKeyPassphrase(Optional<String> privateKeyPassphrase)
        {
            this.privateKeyPassphrase = privateKeyPassphrase;
            return self();
        }

        @CanIgnoreReturnValue
        public Builder withPassword(Optional<String> password)
        {
            this.password = password;
            return self();
        }

        @CanIgnoreReturnValue
        public Builder withDatabase(Optional<String> databaseName)
        {
            this.databaseName = databaseName;
            return self();
        }

        @CanIgnoreReturnValue
        public Builder withCatalog(String catalogName)
        {
            this.catalogName = requireNonNull(catalogName, "catalogName is null");
            return self();
        }

        @CanIgnoreReturnValue
        public Builder withSchema(Optional<String> schemaName)
        {
            this.schemaName = schemaName;
            return self();
        }

        // additive. TODO change name to indicate that
        @CanIgnoreReturnValue
        public Builder withConnectorProperties(Map<String, String> connectorProperties)
        {
            this.connectorProperties.putAll(requireNonNull(connectorProperties, "connectorProperties is null"));
            return self();
        }

        @CanIgnoreReturnValue
        public Builder withNodeCount(int nodeCount)
        {
            this.nodeCount = nodeCount;
            return self();
        }

        @CanIgnoreReturnValue
        public Builder withTpchTables(Iterable<TpchTable<?>> tpchTables)
        {
            this.tpchTables = tpchTables;
            return self();
        }

        @CanIgnoreReturnValue
        public Builder withCreateUserContextView()
        {
            verify(databaseName.isPresent(), "Database name must be provided to create view");
            // Create view used for testing user/role impersonation
            SnowflakeServer.safeExecuteOnDatabase(
                    databaseName.get(),
                    "CREATE VIEW IF NOT EXISTS public.user_context (user, role) AS SELECT current_user(), current_role();",
                    "GRANT SELECT ON VIEW USER_CONTEXT TO ROLE \"PUBLIC\";");
            return self();
        }

        @Override
        public DistributedQueryRunner build()
                throws Exception
        {
            if (databaseName.isPresent() && schemaName.isPresent()) {
                SnowflakeServer.createSchema(databaseName.get(), schemaName.get());
            }

            amendSession(sessionBuilder -> sessionBuilder.setCatalog(catalogName));
            setWorkerCount(nodeCount - 1);
            DistributedQueryRunner queryRunner = super.build();
            Session session = queryRunner.getDefaultSession();
            try {
                queryRunner.installPlugin(new TpchPlugin());
                queryRunner.createCatalog(TPCH_CATALOG, TPCH_CATALOG, ImmutableMap.of());

                ImmutableMap.Builder<String, String> properties = ImmutableMap.<String, String>builder()
                        .put("connection-url", JDBC_URL)
                        .put("connection-user", USER)
                        .putAll(connectorProperties.buildOrThrow());
                password.ifPresent(password -> properties.put("connection-password", password));
                privateKey.ifPresent(key -> properties.put("snowflake.connection-private-key", key));
                privateKeyPassphrase.ifPresent(passphrase -> properties.put("snowflake.connection-private-key.passphrase", passphrase));

                warehouseName.ifPresent(warehouse -> properties.put("snowflake.warehouse", warehouse));
                databaseName.ifPresent(database -> properties.put("snowflake.database", database));

                queryRunner.installPlugin(new TestingSnowflakePlugin());
                queryRunner.createCatalog(catalogName, connectorName, properties.buildOrThrow());

                copyTpchTables(queryRunner, TPCH_CATALOG, TINY_SCHEMA_NAME, session, tpchTables);

                queryRunner.installPlugin(new JmxPlugin());
                queryRunner.createCatalog("jmx", "jmx", ImmutableMap.of());
            }
            catch (Throwable e) {
                closeAllSuppress(e, queryRunner);
                throw e;
            }
            return queryRunner;
        }
    }

    protected SnowflakeQueryRunner() {}

    static void main()
            throws Exception
    {
        Logging.initialize();

        DistributedQueryRunner queryRunner = parallelBuilder()
                .withConnectorProperties(ImmutableMap.<String, String>builder()
                        .putAll(impersonationDisabled())
                        .buildOrThrow())
                .addCoordinatorProperties(ImmutableMap.of("http-server.http.port", "8080"))
                .build();

        Logger log = Logger.get(SnowflakeQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
