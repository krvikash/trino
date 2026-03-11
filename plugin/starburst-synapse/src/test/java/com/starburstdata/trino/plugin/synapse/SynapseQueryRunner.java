/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.synapse;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Module;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.Session;
import io.trino.metadata.QualifiedObjectName;
import io.trino.plugin.jmx.JmxPlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.security.Identity;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import static com.starburstdata.trino.plugin.synapse.SynapseServer.JDBC_URL;
import static com.starburstdata.trino.plugin.synapse.SynapseServer.PASSWORD;
import static com.starburstdata.trino.plugin.synapse.SynapseServer.USERNAME;
import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.airlift.units.Duration.nanosSince;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.TimeUnit.SECONDS;

public final class SynapseQueryRunner
{
    private SynapseQueryRunner() {}

    private static final Logger log = Logger.get(SynapseQueryRunner.class);

    private static final int ERROR_OBJECT_EXISTS = 2714;

    public static final String DEFAULT_CATALOG_NAME = "synapse";
    public static final String TEST_SCHEMA = "dbo";

    public static DistributedQueryRunner createSynapseQueryRunner(
            SynapseServer synapseServer,
            Map<String, String> connectorProperties,
            Iterable<TpchTable<?>> tables)
            throws Exception
    {
        return createSynapseQueryRunner(
                Map.of(),
                synapseServer,
                DEFAULT_CATALOG_NAME,
                connectorProperties,
                tables);
    }

    public static DistributedQueryRunner createSynapseQueryRunner(
            Map<String, String> coordinatorProperties,
            SynapseServer synapseServer,
            String catalogName,
            Map<String, String> connectorProperties,
            Iterable<TpchTable<?>> tables)
            throws Exception
    {
        return createSynapseQueryRunner(Map.of(), synapseServer, catalogName, connectorProperties, coordinatorProperties, tables, Optional.empty(), runner -> {});
    }

    public static DistributedQueryRunner createSynapseQueryRunner(
            Map<String, String> extraProperties,
            SynapseServer synapseServer,
            String catalogName,
            Map<String, String> connectorProperties,
            Map<String, String> coordinatorProperties,
            Iterable<TpchTable<?>> tables,
            Optional<Module> failureInjectionModule,
            Consumer<DistributedQueryRunner.Builder<?>> moreSetup)
            throws Exception
    {
        Session session = createSession(USERNAME, catalogName);
        DistributedQueryRunner.Builder<?> queryRunnerBuilder = DistributedQueryRunner.builder(session)
                .setExtraProperties(extraProperties)
                .setCoordinatorProperties(coordinatorProperties);
        moreSetup.accept(queryRunnerBuilder);
        failureInjectionModule.ifPresent(queryRunnerBuilder::setAdditionalModule);
        DistributedQueryRunner queryRunner = queryRunnerBuilder.build();
        try {
            queryRunner.installPlugin(new JmxPlugin());
            queryRunner.createCatalog("jmx", "jmx");

            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");

            connectorProperties = new HashMap<>(ImmutableMap.copyOf(connectorProperties));
            connectorProperties.putIfAbsent("connection-url", JDBC_URL);
            connectorProperties.putIfAbsent("connection-user", USERNAME);
            connectorProperties.putIfAbsent("connection-password", PASSWORD);
            connectorProperties.putIfAbsent("connection-pool.max-size", String.valueOf(maxPoolSize()));

            try {
                synapseServer.execute(format(
                        "CREATE VIEW %s.user_context AS SELECT " +
                                "SESSION_USER AS session_user_column," +
                                "CURRENT_USER AS current_user_column",
                        TEST_SCHEMA));
            }
            catch (RuntimeException e) {
                if (!(e.getCause() instanceof SQLServerException) || ((SQLServerException) e.getCause()).getErrorCode() != ERROR_OBJECT_EXISTS) {
                    throw e;
                }
            }

            queryRunner.installPlugin(new TestingSynapsePlugin());

            queryRunner.createCatalog(catalogName, "synapse", connectorProperties);

            copyTpchTablesIfNotExists(queryRunner, "tpch", TINY_SCHEMA_NAME, session, tables);

            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    private static int maxPoolSize()
    {
        // Assume JUnit will run one test per processor concurrently, which is the default behavior.
        // https://docs.junit.org/6.0.2/writing-tests/parallel-execution.html
        int estimatedConcurrentTests = Runtime.getRuntime().availableProcessors();
        // Each test running in a single thread could require more than one concurrent connection;
        // allowing more connections than tests running makes deadlocks across tests less likely.
        // Then clamp the max pool size to at least 10 since that's the default max pool size.
        return Integer.max(estimatedConcurrentTests * 2, 10);
    }

    public static Session createSession(String user, String catalog)
    {
        return testSessionBuilder()
                .setCatalog(catalog)
                .setSchema(TEST_SCHEMA)
                .setIdentity(Identity.ofUser(user))
                .build();
    }

    private static void copyTpchTablesIfNotExists(
            QueryRunner queryRunner,
            String sourceCatalog,
            String sourceSchema,
            Session session,
            Iterable<TpchTable<?>> tables)
    {
        log.info("Loading data from %s.%s...", sourceCatalog, sourceSchema);
        long startTime = System.nanoTime();
        for (TpchTable<?> table : tables) {
            copyTableIfNotExist(queryRunner, sourceCatalog, sourceSchema, table.getTableName().toLowerCase(ENGLISH), session);
        }
        log.info("Loading from %s.%s complete in %s", sourceCatalog, sourceSchema, nanosSince(startTime).toString(SECONDS));
    }

    // CREATE TABLE IF NOT EXISTS isn't an atomic operation, so multiple tests running concurrently
    // pointing at the same database can lead to race conditions and failures from trying to create
    // a table that already exists. Synchronizing prevents this within a given test process.
    // Additional care must also be taken to avoid concurrent test initializations across different
    // processes if they are using the same database.
    // We could synchronize more granularly, e.g. a lock per table name, but this is just used to
    // initialize tests so isn't worth that work and complexity.
    private static synchronized void copyTableIfNotExist(QueryRunner queryRunner, String sourceCatalog, String sourceSchema, String sourceTable, Session session)
    {
        QualifiedObjectName table = new QualifiedObjectName(sourceCatalog, sourceSchema, sourceTable);
        long start = System.nanoTime();
        log.info("Running import for %s", table.objectName());
        String sql = format("CREATE TABLE IF NOT EXISTS %s AS SELECT * FROM %s", table.objectName(), table);
        long rows = (Long) queryRunner.execute(session, sql).getMaterializedRows().get(0).getField(0);
        log.info("Imported %s rows for %s in %s", rows, table.objectName(), nanosSince(start).convertToMostSuccinctTimeUnit());
    }

    static void main()
            throws Exception
    {
        Logging.initialize();

        @SuppressWarnings("resource")
        DistributedQueryRunner queryRunner = createSynapseQueryRunner(
                Map.of("http-server.http.port", "8080"),
                new SynapseServer(), // dummy
                DEFAULT_CATALOG_NAME,
                Map.of(),
                TpchTable.getTables());

        Logger log = Logger.get(SynapseQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
