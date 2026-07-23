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
import com.google.common.collect.Streams;
import com.google.inject.Module;
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
import java.util.Set;
import java.util.function.Consumer;

import static com.starburstdata.trino.plugin.synapse.SynapseServer.JDBC_URL;
import static com.starburstdata.trino.plugin.synapse.SynapseServer.PASSWORD;
import static com.starburstdata.trino.plugin.synapse.SynapseServer.TEST_SCHEMA;
import static com.starburstdata.trino.plugin.synapse.SynapseServer.USERNAME;
import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.airlift.units.Duration.nanosSince;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.TestingProperties.requiredNonEmptySystemProperty;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toUnmodifiableSet;

public final class SynapseQueryRunner
{
    private SynapseQueryRunner() {}

    private static final Logger log = Logger.get(SynapseQueryRunner.class);

    private static final String AZURE_STORAGE_TPCH_TABLES_ROOT = requiredNonEmptySystemProperty("test.synapse.azure.storage.tpch.tables.root");

    private static final int ERROR_OBJECT_EXISTS = 2714;

    public static final String DEFAULT_CATALOG_NAME = "synapse";

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
        return createSynapseQueryRunner(Map.of(), synapseServer, catalogName, connectorProperties, coordinatorProperties, tables, Optional.empty(), _ -> {});
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

            synapseServer.executeIgnoringErrors(format(
                    "CREATE VIEW %s.user_context AS SELECT " +
                            "SESSION_USER AS session_user_column," +
                            "CURRENT_USER AS current_user_column",
                    TEST_SCHEMA), ERROR_OBJECT_EXISTS);

            queryRunner.installPlugin(new TestingSynapsePlugin());

            queryRunner.createCatalog(catalogName, "synapse", connectorProperties);

            copyTpchTablesIfNotExists(synapseServer, queryRunner, "tpch", TINY_SCHEMA_NAME, session, tables);

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

    // Synchronizing prevents race conditions within a given test process when multiple tests
    // concurrently initialize against the same database. Additional care must be taken to avoid
    // concurrent initializations across different processes sharing the same database.
    private static synchronized void copyTpchTablesIfNotExists(
            SynapseServer synapseServer,
            QueryRunner queryRunner,
            String sourceCatalog,
            String sourceSchema,
            Session session,
            Iterable<TpchTable<?>> tables)
    {
        log.info("Loading data from %s.%s...", sourceCatalog, sourceSchema);
        long startTime = System.nanoTime();

        Set<String> existingTables = queryRunner.listTables(session, session.getCatalog().orElseThrow(), session.getSchema().orElseThrow())
                .stream()
                .map(QualifiedObjectName::objectName)
                .collect(toUnmodifiableSet());

        Streams.stream(tables)
                .map(table -> table.getTableName().toLowerCase(ENGLISH))
                .filter(name -> !existingTables.contains(name))
                .forEach(name -> copyTable(synapseServer, queryRunner, sourceCatalog, sourceSchema, name, session));

        log.info("Loading from %s.%s complete in %s", sourceCatalog, sourceSchema, nanosSince(startTime).toString(SECONDS));
    }

    private static void copyTable(SynapseServer synapseServer, QueryRunner queryRunner, String sourceCatalog, String sourceSchema, String tableName, Session session)
    {
        String storagePath = format("%s/%s/%s/%s/", AZURE_STORAGE_TPCH_TABLES_ROOT, sourceCatalog, sourceSchema, tableName);
        log.info("Creating table %s.%s in Synapse copying from %s", session.getSchema().orElseThrow(), tableName, storagePath);

        queryRunner.execute(session, format(
                "CREATE TABLE %s.%s.%s AS SELECT * FROM %s.%s.%s WITH NO DATA",
                session.getCatalog().orElseThrow(),
                session.getSchema().orElseThrow(),
                tableName,
                sourceCatalog,
                sourceSchema,
                tableName));

        synapseServer.executeAsOwner(
                format("COPY INTO %s.%s FROM '%s' WITH (FILE_TYPE = 'PARQUET', CREDENTIAL = (IDENTITY = 'Managed Identity'))",
                        TEST_SCHEMA,
                        tableName,
                        storagePath),
                null);
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
