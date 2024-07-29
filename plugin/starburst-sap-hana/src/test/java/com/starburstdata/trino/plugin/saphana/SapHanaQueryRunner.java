/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.saphana;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.plugin.jmx.JmxPlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.tpch.TpchTable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.QueryAssertions.copyTpchTables;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.Boolean.parseBoolean;
import static java.lang.System.getenv;
import static java.util.Objects.requireNonNull;

public final class SapHanaQueryRunner
{
    public static final String GRANTED_USER = "alice";
    public static final String NON_GRANTED_USER = "bob";
    private static final boolean TESTCONTAINERS_REUSE_ENABLE = parseBoolean(getenv("TESTCONTAINERS_REUSE_ENABLE"));

    private SapHanaQueryRunner() {}

    public static Builder builder(TestingSapHanaServer server)
    {
        return new Builder(server);
    }

    public static final class Builder
            extends DistributedQueryRunner.Builder<Builder>
    {
        private final TestingSapHanaServer server;
        private Map<String, String> connectorProperties = new HashMap<>();
        private List<TpchTable<?>> initialTables = ImmutableList.of();

        private Builder(TestingSapHanaServer server)
        {
            super(testSessionBuilder()
                    .setCatalog("saphana")
                    .setSchema("tpch")
                    .build());
            this.server = requireNonNull(server, "server is null");
        }

        @CanIgnoreReturnValue
        public Builder addConnectorProperty(String key, String value)
        {
            this.connectorProperties.put(key, value);
            return this;
        }

        @CanIgnoreReturnValue
        public Builder setInitialTables(Iterable<TpchTable<?>> initialTables)
        {
            this.initialTables = ImmutableList.copyOf(requireNonNull(initialTables, "initialTables is null"));
            return this;
        }

        @Override
        public DistributedQueryRunner build()
                throws Exception
        {
            DistributedQueryRunner queryRunner = super.build();
            try {
                queryRunner.installPlugin(new JmxPlugin());
                queryRunner.createCatalog("jmx", "jmx");

                queryRunner.installPlugin(new TpchPlugin());
                queryRunner.createCatalog("tpch", "tpch");

                connectorProperties = new HashMap<>(ImmutableMap.copyOf(connectorProperties));
                connectorProperties.putIfAbsent("connection-url", server.getJdbcUrl());
                connectorProperties.putIfAbsent("connection-user", server.getUser());
                connectorProperties.putIfAbsent("connection-password", server.getPassword());

                List<String> queries = ImmutableList.of(
                        "CREATE SCHEMA tpch",
                        "CREATE USER " + GRANTED_USER,
                        "CREATE USER " + NON_GRANTED_USER,
                        "GRANT ALL PRIVILEGES ON SCHEMA tpch TO " + GRANTED_USER);

                if (TESTCONTAINERS_REUSE_ENABLE) {
                    try {
                        // in case when environment was already initialized by previous run, queries will fail,
                        // and server::executeWithRetry will retry, and, still, eventually will fail.
                        // this leads to longer startup time for local environment.
                        // To mitigate that single try with server::execute is used, which is the acceptable trade-off during local testing.
                        queries.forEach(server::execute);
                    }
                    catch (RuntimeException e) {
                        System.err.println("Environment was not initialized properly. Either because local environment was already initialized by previous run, or... it's actual fail."
                                + " TESTCONTAINERS_REUSE_ENABLE=true, so you know what you do." + e.getMessage());
                    }
                }
                else {
                    queries.forEach(server::executeWithRetry);
                }
                queryRunner.installPlugin(new TestingSapHanaPlugin());
                queryRunner.createCatalog("saphana", "sap_hana", connectorProperties);

                copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, queryRunner.getDefaultSession(), initialTables);

                return queryRunner;
            }
            catch (Throwable e) {
                closeAllSuppress(e, queryRunner);
                throw e;
            }
        }
    }

    public static void main(String[] args)
            throws Exception
    {
        Logging.initialize();

        TestingSapHanaServer sapHanaServer = TestingSapHanaServer.create();
        DistributedQueryRunner queryRunner = SapHanaQueryRunner.builder(sapHanaServer)
                .setCoordinatorProperties(ImmutableMap.of("http-server.http.port", "8080"))
                .setInitialTables(TpchTable.getTables())
                .build();

        Logger log = Logger.get(SapHanaQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
