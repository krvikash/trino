/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.salesforce;

import com.google.common.collect.ImmutableMap;
import com.starburstdata.trino.plugin.license.LicenseVerifier;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.Session;
import io.trino.metadata.QualifiedObjectName;
import io.trino.plugin.jmx.JmxPlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchColumn;
import io.trino.tpch.TpchTable;
import org.intellij.lang.annotations.Language;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.airlift.units.Duration.nanosSince;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;

public final class SalesforceQueryRunner
{
    public static final LicenseVerifier NOOP_LICENSE_MANAGER = () -> true;
    public static final String TIMESTAMP_PUSH_DOWN_TABLE_NAME = "test_timestamp_pushdown";

    private static final Logger log = Logger.get(SalesforceQueryRunner.class);

    static final String SALESFORCE_BASIC_AUTH_USER = requireNonNull(System.getProperty("salesforce.test.basic.auth.user"), "salesforce.test.basic.auth.user is not set");
    static final String SALESFORCE_BASIC_AUTH_PASSWORD = requireNonNull(System.getProperty("salesforce.test.basic.auth.password"), "salesforce.test.basic.auth.password is not set");
    static final String SALESFORCE_BASIC_AUTH_SECURITY_TOKEN = requireNonNull(System.getProperty("salesforce.test.basic.auth.security-token"), "salesforce.test.basic.auth.security-token is not set");
    static final String SALESFORCE_BASIC_AUTH_SANDBOX_ENABLED = "true";

    private SalesforceQueryRunner() {}

    public static Builder builder()
    {
        return new Builder();
    }

    public static Session createSession(String catalogName)
    {
        return testSessionBuilder()
                .setCatalog(catalogName)
                .setSchema("salesforce")
                .build();
    }

    private static DistributedQueryRunner createQueryRunner(
            Map<String, String> coordinatorProperties,
            String catalogName,
            Map<String, String> connectorProperties,
            Map<String, TpchTable<?>> tableNameMapper,
            boolean enableWrites)
            throws Exception
    {
        // Copy tables first before creating the query runner
        // We need to enable writes to copy but the returned query runner will enable writes based on the given parameter
        // We also only copy the tables if they exist
        // Deleted tables from Salesforce are not actually deleted for 15 days
        // As the CI builds times, the sandbox would quickly fill up and then the builds will fail
        // We also don't want to hit our API limit, so instead we just create the tables once but will assert
        // all the data is in the tables each CI run
        copyTestTablesIfNotExists(coordinatorProperties, catalogName, connectorProperties, tableNameMapper);

        DistributedQueryRunner queryRunner = null;
        try {
            DistributedQueryRunner.Builder<?> builder = DistributedQueryRunner.builder(createSession(catalogName));
            coordinatorProperties.forEach(builder::addCoordinatorProperty);
            queryRunner = builder.build();

            connectorProperties = new HashMap<>(ImmutableMap.copyOf(connectorProperties));

            queryRunner.installPlugin(new TestingSalesforcePlugin(enableWrites));
            queryRunner.createCatalog(catalogName, "salesforce", connectorProperties);

            queryRunner.installPlugin(new JmxPlugin());
            queryRunner.createCatalog("jmx", "jmx");

            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");

            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    private static void truncateTable(String tableName)
    {
        SalesforcePasswordConfig passwordConfig = new SalesforcePasswordConfig()
                .setUser(SALESFORCE_BASIC_AUTH_USER)
                .setPassword(SALESFORCE_BASIC_AUTH_PASSWORD)
                .setSecurityToken(SALESFORCE_BASIC_AUTH_SECURITY_TOKEN);

        SalesforceConfig config = new SalesforceConfig()
                .setSandboxEnabled(true);

        // Query Salesforce to get all of the Id columns for the rows and insert them into a temp table
        // which stores the data in-memory in the driver
        // Then use DELETE FROM the temp table to issue batched deletes to Salesforce
        String jdbcUrl = new SalesforceModule.PasswordConnectionUrlProvider(config, passwordConfig).get();
        try (Connection connection = DriverManager.getConnection(jdbcUrl);
                Statement statement = connection.createStatement();
                PreparedStatement preparedStatement = connection.prepareStatement(format("INSERT INTO %s__c#TEMP (Id) VALUES (?)", tableName));
                ResultSet results = statement.executeQuery(format("SELECT Id FROM %s__c", tableName))) {
            boolean hasData = false;
            while (results.next()) {
                hasData = true;
                preparedStatement.setObject(1, results.getObject(1));
                preparedStatement.execute();
            }

            if (hasData) {
                statement.execute(format("DELETE FROM %s__c WHERE EXISTS SELECT Id FROM %s__c#TEMP", tableName, tableName));
            }

            // Assert the table is empty
            try (ResultSet countResults = statement.executeQuery(format("SELECT COUNT(*) FROM %s__c", tableName))) {
                countResults.next();
                int numRows = countResults.getInt(1);
                assertThat(numRows)
                        .as(format("Table %s has %s rows but expected 0", tableName, numRows))
                        .isEqualTo(0);
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Error truncating table", e);
        }
    }

    private static void copyTestTablesIfNotExists(Map<String, String> coordinatorProperties, String catalogName, Map<String, String> connectorProperties, Map<String, TpchTable<?>> tableNameMapper)
            throws Exception
    {
        DistributedQueryRunner.Builder<?> builder = DistributedQueryRunner.builder(createSession(catalogName));
        coordinatorProperties.forEach(builder::addCoordinatorProperty);
        try (DistributedQueryRunner queryRunner = builder.build()) {
            connectorProperties = new HashMap<>(ImmutableMap.copyOf(connectorProperties));

            queryRunner.installPlugin(new TestingSalesforcePlugin(true));
            queryRunner.createCatalog(catalogName, "salesforce", connectorProperties);

            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");

            copyTestTablesIfNotExists(queryRunner, "tpch", TINY_SCHEMA_NAME, createSession(catalogName), tableNameMapper);
        }
    }

    private static void copyTestTablesIfNotExists(
            QueryRunner queryRunner,
            String sourceCatalog,
            String sourceSchema,
            Session session,
            Map<String, TpchTable<?>> tableNameMapper)
    {
        log.info("Loading data from %s.%s...", sourceCatalog, sourceSchema);
        long startTime = System.nanoTime();
        for (Map.Entry<String, TpchTable<?>> tableEntry : tableNameMapper.entrySet()) {
            QualifiedObjectName salesforceQualifiedTable = new QualifiedObjectName(sourceCatalog, sourceSchema, tableEntry.getKey().toLowerCase(ENGLISH));
            QualifiedObjectName tpchQualifiedTable = new QualifiedObjectName(sourceCatalog, sourceSchema, tableEntry.getValue().getTableName().toLowerCase(ENGLISH));
            copyTableIfNotExists(queryRunner, tableEntry.getValue(), tpchQualifiedTable, salesforceQualifiedTable, session);
        }

        log.info("Loading from %s.%s complete in %s", sourceCatalog, sourceSchema, nanosSince(startTime).toString(SECONDS));
    }

    private static void copyTableIfNotExists(QueryRunner queryRunner, TpchTable<?> table, QualifiedObjectName tpchQualifiedTable, QualifiedObjectName salesforceQualifiedTable, Session session)
    {
        // Check if the table exists rather than running CREATE TABLE IF NOT EXISTS because the Salesforce table name has the suffix
        // The SQL query would fail because the e.g. 'customer' table doesn't exist, but then you get an error
        // trying to create the same 'customer__c' object in Salesforce

        // We assert the row count if it does exist to check if it is loaded correctly
        // If not, the table is truncated and then re-loaded

        if (queryRunner.execute(format("SHOW TABLES LIKE '%s__c'", TIMESTAMP_PUSH_DOWN_TABLE_NAME)).getRowCount() == 0) {
            String salesforceObjectNameForTable = TIMESTAMP_PUSH_DOWN_TABLE_NAME + "__c";
            log.info("Table %s does not exist, running CTAS", salesforceObjectNameForTable);
            String sql =
                    """
                            CREATE TABLE %s (id, ts) AS VALUES
                            (CAST('0' AS VARCHAR(1)),  TIMESTAMP '2020-10-26 11:02:00 UTC'),
                            (CAST('1' AS VARCHAR(1)),  TIMESTAMP '2020-10-26 11:02:01 UTC'),
                            (CAST('2' AS VARCHAR(1)),  TIMESTAMP '2020-10-26 11:02:02 UTC'),
                            (CAST('3' AS VARCHAR(1)),  TIMESTAMP '2020-10-26 11:02:03 UTC')""".formatted(TIMESTAMP_PUSH_DOWN_TABLE_NAME);
            long start = System.nanoTime();
            log.info("Running import for %s", salesforceObjectNameForTable);
            long rows = (Long) queryRunner.execute(session, sql).getMaterializedRows().get(0).getField(0);
            log.info("Imported %s rows for %s in %s", rows, salesforceQualifiedTable.objectName(), nanosSince(start).convertToMostSuccinctTimeUnit());
        }
        else {
            log.info("Table %s already exists, skipping CTAS", TIMESTAMP_PUSH_DOWN_TABLE_NAME);
        }
        @Language("SQL") String sql;
        if (!queryRunner.tableExists(session, salesforceQualifiedTable.objectName() + "__c")) {
            log.info("Table %s does not exist, running CTAS", salesforceQualifiedTable.objectName());
            sql = format("CREATE TABLE %s AS SELECT * FROM %s", salesforceQualifiedTable.objectName(), tpchQualifiedTable);
        }
        else {
            log.info("Table %s exists, checking row count", salesforceQualifiedTable.objectName());
            long expectedCount = (long) queryRunner.execute(session, "SELECT count(*) FROM " + tpchQualifiedTable).getOnlyValue();
            long actualCount = (long) queryRunner.execute(session, "SELECT count(*) FROM " + salesforceQualifiedTable.objectName() + "__c").getOnlyValue();

            if (expectedCount == actualCount) {
                log.info("Table %s already exists and is loaded correctly", salesforceQualifiedTable.objectName());
                return;
            }

            log.info("Table %s exists, truncating table and reloading data", salesforceQualifiedTable.objectName());
            truncateTable(salesforceQualifiedTable.objectName().toLowerCase(ENGLISH));

            String columnDefinition = table.getColumns().stream().map(TpchColumn::getSimplifiedColumnName).collect(joining("__c, ", "", "__c"));
            String columnMappings = table.getColumns().stream().map(TpchColumn::getSimplifiedColumnName).map(name -> format("%s AS %s__c", name, name)).collect(joining(", "));
            sql = format("INSERT INTO %s__c (%s) SELECT %s FROM %s", salesforceQualifiedTable.objectName(), columnDefinition, columnMappings, tpchQualifiedTable);
        }

        // Run either the CREATE or INSERT and assert that it is loaded correctly, failing if it is not
        long start = System.nanoTime();
        log.info("Running import for %s", salesforceQualifiedTable.objectName());
        long rows = (Long) queryRunner.execute(session, sql).getMaterializedRows().get(0).getField(0);
        log.info("Imported %s rows for %s in %s", rows, salesforceQualifiedTable.objectName(), nanosSince(start).convertToMostSuccinctTimeUnit());

        log.info("Running assertion for %s", salesforceQualifiedTable.objectName());
        assertThat(queryRunner.execute(session, "SELECT count(*) FROM " + tpchQualifiedTable).getOnlyValue())
                .as("Table is not loaded properly: %s", salesforceQualifiedTable)
                .isEqualTo(queryRunner.execute(session, "SELECT count(*) FROM " + salesforceQualifiedTable.objectName() + "__c").getOnlyValue());
    }

    public static class Builder
    {
        private Map<String, TpchTable<?>> tableNameMapper = Map.of();
        private String catalogName = "salesforce";
        private Map<String, String> connectorProperties;
        private Map<String, String> coordinatorProperties;
        private boolean enableWrites;

        public Builder()
        {
            connectorProperties = ImmutableMap.<String, String>builder()
                    .put("salesforce.user", SALESFORCE_BASIC_AUTH_USER)
                    .put("salesforce.password", SALESFORCE_BASIC_AUTH_PASSWORD)
                    .put("salesforce.security-token", SALESFORCE_BASIC_AUTH_SECURITY_TOKEN)
                    .put("salesforce.enable-sandbox", SALESFORCE_BASIC_AUTH_SANDBOX_ENABLED)
                    .buildOrThrow();
            coordinatorProperties = ImmutableMap.of();
        }

        public Builder setCatalogName(String catalogName)
        {
            this.catalogName = requireNonNull(catalogName, "catalogName is null");
            return this;
        }

        public Builder addConnectorProperties(Map<String, String> properties)
        {
            connectorProperties = updateProperties(connectorProperties, properties);
            return this;
        }

        public Builder addCoordinatorProperties(Map<String, String> properties)
        {
            coordinatorProperties = updateProperties(coordinatorProperties, properties);
            return this;
        }

        public Builder setTableNameMapper(Map<String, TpchTable<?>> tableNameMapper)
        {
            this.tableNameMapper = requireNonNull(tableNameMapper, "tableNameMapper is null");
            return this;
        }

        public Builder enableWrites()
        {
            this.enableWrites = true;
            return this;
        }

        public Builder enableDriverLogging()
        {
            addConnectorProperties(ImmutableMap.of("salesforce.driver-logging.enabled", "true"));
            return this;
        }

        public DistributedQueryRunner build()
                throws Exception
        {
            return createQueryRunner(
                    coordinatorProperties,
                    catalogName,
                    connectorProperties,
                    tableNameMapper,
                    enableWrites);
        }

        private static Map<String, String> updateProperties(Map<String, String> properties, Map<String, String> update)
        {
            return ImmutableMap.<String, String>builder()
                    .putAll(requireNonNull(properties, "properties is null"))
                    .putAll(requireNonNull(update, "update is null"))
                    .buildOrThrow();
        }
    }

    static void main()
            throws Exception
    {
        Logging.initialize();

        DistributedQueryRunner queryRunner = SalesforceQueryRunner.builder()
                .enableWrites()
                .addCoordinatorProperties(ImmutableMap.of("http-server.http.port", "8080"))
                .build();

        Logger log = Logger.get(SalesforceQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
