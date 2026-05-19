/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.objectstore;

import com.google.common.collect.ImmutableMap;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.Session;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.s3.S3FileSystemConfig;
import io.trino.filesystem.s3.S3FileSystemFactory;
import io.trino.filesystem.s3.S3FileSystemStats;
import io.trino.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.thrift.BridgingHiveMetastore;
import io.trino.plugin.iceberg.IcebergPlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.Plugin;
import io.trino.testing.BaseConnectorTest;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.TestTable;
import org.assertj.core.api.AbstractThrowableAssert;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.plugin.hive.TestingThriftHiveMetastoreBuilder.testingThriftHiveMetastoreBuilder;
import static io.trino.plugin.objectstore.MinioStorage.ACCESS_KEY;
import static io.trino.plugin.objectstore.MinioStorage.REGION;
import static io.trino.plugin.objectstore.MinioStorage.SECRET_KEY;
import static io.trino.plugin.objectstore.StarburstObjectStoreConnectorFactory.STARBURST_OBJECTSTORE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.DROP_TABLE;
import static io.trino.testing.TestingAccessControlManager.privilege;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_ADD_COLUMN;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_ADD_FIELD;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_CREATE_OR_REPLACE_TABLE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_CREATE_TABLE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_CREATE_TABLE_WITH_DATA;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_CREATE_VIEW;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_RENAME_COLUMN;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_RENAME_TABLE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_ROW_TYPE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_SET_COLUMN_TYPE;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.Integer.parseInt;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.abort;

public abstract class BaseObjectStoreConnectorTest
        extends BaseConnectorTest
{
    protected final String bucketName = "test-bucket-" + randomNameSuffix();
    protected final boolean isGalaxyMetastore;
    private final TableType tableType;
    protected MinioStorage minio;
    protected HiveMetastore metastore;

    protected BaseObjectStoreConnectorTest(boolean isGalaxyMetastore, TableType tableType)
    {
        this.isGalaxyMetastore = isGalaxyMetastore;
        this.tableType = requireNonNull(tableType, "tableType is null");
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        if (isGalaxyMetastore) {
            return abort("Galaxy only");
        }
        else {
            return createStarburstObjectStoreQueryRunner();
        }
    }

    private QueryRunner createStarburstObjectStoreQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(
                        testSessionBuilder()
                                .setCatalog("objectstore")
                                .setSchema("tpch")
                                .build())
                .build();
        try {
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch", Map.of());

            HiveMinioStorage hiveMinio = closeAfterClass(new HiveMinioStorage(bucketName));
            hiveMinio.start();
            minio = hiveMinio.minioStorage();

            metastore = new BridgingHiveMetastore(
                    testingThriftHiveMetastoreBuilder()
                            .metastoreClient(hiveMinio.hiveMetastoreEndpoint())
                            .build(this::closeAfterClass));

            queryRunner.installPlugin(new IcebergPlugin());
            queryRunner.installPlugin(new ObjectStorePlugin());
            queryRunner.createCatalog("objectstore", STARBURST_OBJECTSTORE, ImmutableMap.<String, String>builder()
                    .put("great-lakes.table-type", tableType.name())
                    .put("hive.metastore.uri", hiveMinio.hiveMetastoreEndpoint().toString())
                    .put("hive.non-managed-table-writes-enabled", "true")
                    .putAll(minio.getNativeS3Config())
                    .put("iceberg.file-format", "PARQUET")
                    .put("iceberg.add-files-procedure.enabled", "true")
                    .put("iceberg.format-version", "3")
                    .put("iceberg.register-table-procedure.enabled", "true")
                    .put("delta.enable-non-concurrent-writes", "true")
                    .put("delta.register-table-procedure.enabled", "true")
                    .buildOrThrow());
            queryRunner.execute("CREATE SCHEMA objectstore.tpch WITH (location = 's3://" + bucketName + "/tpch')");

            queryRunner.installPlugin(buildMockConnectorPlugin());
            queryRunner.createCatalog("mock_dynamic_listing", "mock", ImmutableMap.of());

            initializeTpchTables(queryRunner, metastore);
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }

        return queryRunner;
    }

    protected Plugin getObjectStorePlugin()
    {
        return new ObjectStorePlugin();
    }

    protected void initializeTpchTables(DistributedQueryRunner queryRunner, HiveMetastore metastore)
            throws Exception
    {
        ObjectStoreQueryRunner.initializeTpchTables(queryRunner, REQUIRED_TPCH_TABLES);
    }

    protected void dropDatabaseFromMetastore(String database)
    {
        metastore.dropDatabase(database, false);
    }

    protected void dropTableFromMetastore(String databaseName, String tableName)
    {
        metastore.dropTable(databaseName, tableName, false);
    }

    // mock catalog init inside create query runner to assign catalog ID to it
    @Override
    public void initMockCatalog() {}

    @Override
    protected abstract boolean hasBehavior(TestingConnectorBehavior connectorBehavior);

    @Override
    protected String createSchemaSql(String schemaName)
    {
        return "CREATE SCHEMA " + schemaName + " WITH (location = 's3://" + bucketName + "/" + schemaName + "')";
    }

    // TODO why we need this? Object Store allows only auto-commit, so the test should pass
    @Override
    protected void assertWriteNotAllowedInTransaction(TestingConnectorBehavior behavior, String sql)
    {
        // skip test since we only support auto-commit
    }

    @Test
    @Override // Override because the error message is different from "This connector does not support creating functions"
    public void testCreateFunction()
    {
        if (!isGalaxyMetastore) {
            super.testCreateFunction();
            return;
        }

        assertThatThrownBy(super::testCreateFunction)
                .hasMessageContaining("Access Denied: Cannot create function");
    }

    @Override
    protected TestTable createTableWithDefaultColumns()
    {
        return abort("Connector does not support column default values");
    }

    @Override
    protected void verifyVersionedQueryFailurePermissible(Exception e)
    {
        assertThat(e).hasMessageMatching("No temporal version history at or before.*|" +
                "This connector does not support versioned tables.*|" +
                "Versioning is only supported for Iceberg and Delta Lake tables|" +
                "This connector does not support reading tables with TIMESTAMP AS OF|" +
                "Delta Lake snapshot ID does not exists: .*|" +
                "Unsupported type for table version: .*");
    }

    @Override
    protected OptionalInt maxTableNameLength()
    {
        return objectStoreTestMaxTableNameLength();
    }

    public static OptionalInt objectStoreTestMaxTableNameLength()
    {
        // Limit table name length for MinIO and Galaxy metastore HTTP API
        // (used to be 255 - UUID.randomUUID().toString().length() but minio/minio:RELEASE.2022-10-05T14-58-27Z tightened limit)
        return OptionalInt.of(240 - UUID.randomUUID().toString().length());
    }

    @Override
    protected OptionalInt maxSchemaNameLength()
    {
        return objectStoreTestMaxSchemaNameLength();
    }

    public static OptionalInt objectStoreTestMaxSchemaNameLength()
    {
        // Limit schema name length for MinIO and Galaxy metastore HTTP API
        // (used to be 255 - UUID.randomUUID().toString().length() but minio/minio:RELEASE.2022-10-05T14-58-27Z tightened limit)
        return OptionalInt.of(240 - UUID.randomUUID().toString().length());
    }

    // Override and disable the negative tests for long schema and table names, because galaxy metastore has no problems storing them
    @Test
    @Override
    public void testCreateSchemaWithLongName()
    {
        String baseSchemaName = "test_create_" + randomNameSuffix();

        int maxLength = maxSchemaNameLength()
                // Assume 2^16 is enough for most use cases. Add a bit more to ensure 2^16 isn't actual limit.
                .orElse(65536 + 5);

        String validSchemaName = baseSchemaName + "z".repeat(maxLength - baseSchemaName.length());
        assertUpdate("CREATE SCHEMA " + validSchemaName + " WITH (location = 's3://" + bucketName + "/" + baseSchemaName + "')");
        assertThat(computeActual("SHOW SCHEMAS").getOnlyColumnAsSet()).contains(validSchemaName);
        assertUpdate("DROP SCHEMA " + validSchemaName);
    }

    @Test
    @Override
    public void testRenameSchemaToLongName()
    {
        String sourceTableName = "test_rename_source_" + randomNameSuffix();
        assertUpdate("CREATE SCHEMA " + sourceTableName);

        String baseSchemaName = "test_rename_target_" + randomNameSuffix();

        int maxLength = maxSchemaNameLength()
                // Assume 2^16 is enough for most use cases. Add a bit more to ensure 2^16 isn't actual limit.
                .orElse(65536 + 5);

        String validTargetSchemaName = baseSchemaName + "z".repeat(maxLength - baseSchemaName.length());
        assertUpdate("ALTER SCHEMA " + sourceTableName + " RENAME TO " + validTargetSchemaName);
        assertThat(computeActual("SHOW SCHEMAS").getOnlyColumnAsSet()).contains(validTargetSchemaName);
        assertUpdate("DROP SCHEMA " + validTargetSchemaName);
    }

    @Test
    @Override
    public void testCreateTableWithLongTableName()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE));

        String baseTableName = "test_create_" + randomNameSuffix();

        int maxLength = maxTableNameLength()
                // Assume 2^16 is enough for most use cases. Add a bit more to ensure 2^16 isn't actual limit.
                .orElse(65536 + 5);

        String validTableName = baseTableName + "z".repeat(maxLength - baseTableName.length());
        assertUpdate("CREATE TABLE " + validTableName + " (a bigint)");
        assertThat(getQueryRunner().tableExists(getSession(), validTableName)).isTrue();
        assertUpdate("DROP TABLE " + validTableName);
    }

    @Test
    @Override
    public void testRenameTableToLongTableName()
    {
        // TODO (https://github.com/starburstdata/stargate/issues/9925) overridden because it's unknown what table name length would be a problem for ALTER TABLE RENAME TO
        //  currently, the test doesn't test failure when name is too long and this should be fixed

        skipTestUnless(hasBehavior(SUPPORTS_RENAME_TABLE));

        String sourceTableName = "test_rename_source_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + sourceTableName + " AS SELECT 123 x", 1);

        String baseTableName = "test_rename_target_" + randomNameSuffix();

        int maxLength = maxTableNameLength().orElseThrow();

        String validTargetTableName = baseTableName + "z".repeat(maxLength - baseTableName.length());
        assertUpdate("ALTER TABLE " + sourceTableName + " RENAME TO " + validTargetTableName);
        assertThat(getQueryRunner().tableExists(getSession(), validTargetTableName)).isTrue();
        assertQuery("SELECT x FROM " + validTargetTableName, "VALUES 123");
        assertUpdate("DROP TABLE " + validTargetTableName);
    }

    @Override
    protected OptionalInt maxColumnNameLength()
    {
        // Limit column name length for MinIO and Galaxy metastore HTTP API
        return OptionalInt.of(255 - UUID.randomUUID().toString().length());
    }

    @Test
    @Override
    public void testCreateTableWithLongColumnName()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE));

        String tableName = "test_long_column" + randomNameSuffix();
        String basColumnName = "col";

        int maxLength = maxColumnNameLength()
                // Assume 2^16 is enough for most use cases. Add a bit more to ensure 2^16 isn't actual limit.
                .orElse(65536 + 5);

        String validColumnName = basColumnName + "z".repeat(maxLength - basColumnName.length());
        assertUpdate("CREATE TABLE " + tableName + " (" + validColumnName + " bigint)");
        assertThat(columnExists(tableName, validColumnName)).isTrue();
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    @Override
    public void testAlterTableAddLongColumnName()
    {
        skipTestUnless(hasBehavior(SUPPORTS_ADD_COLUMN));

        String tableName = "test_long_column" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 123 x", 1);

        String basColumnName = "col";
        int maxLength = maxColumnNameLength()
                // Assume 2^16 is enough for most use cases. Add a bit more to ensure 2^16 isn't actual limit.
                .orElse(65536 + 5);

        String validTargetColumnName = basColumnName + "z".repeat(maxLength - basColumnName.length());
        assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN " + validTargetColumnName + " int");
        assertThat(getQueryRunner().tableExists(getSession(), tableName)).isTrue();
        assertQuery("SELECT x FROM " + tableName, "VALUES 123");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    @Override
    public void testAlterTableRenameColumnToLongName()
    {
        skipTestUnless(hasBehavior(SUPPORTS_RENAME_COLUMN));

        String tableName = "test_long_column" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 123 x", 1);

        String baseColumnName = "col";
        int maxLength = maxColumnNameLength()
                // Assume 2^16 is enough for most use cases. Add a bit more to ensure 2^16 isn't actual limit.
                .orElse(65536 + 5);

        String validTargetColumnName = baseColumnName + "z".repeat(maxLength - baseColumnName.length());
        assertUpdate("ALTER TABLE " + tableName + " RENAME COLUMN x TO " + validTargetColumnName);
        assertQuery("SELECT " + validTargetColumnName + " FROM " + tableName, "VALUES 123");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testDropSchemaCascadeWithAllType()
    {
        String schemaName = "test_drop_schema_cascade" + randomNameSuffix();
        assertUpdate("CREATE SCHEMA " + schemaName + " WITH (location = 's3://" + bucketName + "/" + schemaName + "')");
        assertThat(computeActual("SHOW SCHEMAS").getOnlyColumnAsSet()).contains(schemaName);

        assertUpdate("CREATE TABLE " + schemaName + ".test_hive(a int) WITH (type = 'HIVE')");
        assertUpdate("CREATE TABLE " + schemaName + ".test_iceberg(a int) WITH (type = 'ICEBERG')");
        assertUpdate("CREATE TABLE " + schemaName + ".test_delta(a int) WITH (type = 'DELTA')");
        assertQueryFails("CREATE TABLE " + schemaName + ".test_hudi(a int) WITH (type = 'HUDI')", "Table creation is not supported for Hudi");
        assertThat(computeActual("SHOW TABLES IN " + schemaName).getOnlyColumnAsSet())
                .contains("test_hive", "test_iceberg", "test_delta");
        assertThat(minio.listObjects(schemaName)).isNotEmpty();

        assertUpdate("DROP SCHEMA " + schemaName + " CASCADE");
        assertThat(computeActual("SHOW SCHEMAS").getOnlyColumnAsSet()).doesNotContain(schemaName);
        assertThat(minio.listObjects(schemaName)).isEmpty();
    }

    @Test
    public void testDropSchemaCascadeFailure()
    {
        // Run exclusively as the test creates invalid table object that may affect other tests.
        executeExclusively(() -> {
            try {
                String schemaName = "test_drop_schema_cascade_failure" + randomNameSuffix();
                assertUpdate("CREATE SCHEMA " + schemaName + " WITH (location = 's3://" + bucketName + "/" + schemaName + "')");
                assertThat(computeActual("SHOW SCHEMAS").getOnlyColumnAsSet()).contains(schemaName);

                // Create a table with system table name to cause query failure during dropping schema
                assertUpdate("CREATE TABLE " + schemaName + ".\"test_system_table$partitions\"(a int) WITH (type = 'HIVE')");
                assertUpdate("CREATE VIEW " + schemaName + ".test_view AS SELECT 1 a");
                assertUpdate("CREATE MATERIALIZED VIEW " + schemaName + ".test_materialized_view AS SELECT 1 a");

                assertQueryFails("DROP SCHEMA " + schemaName + " CASCADE", "Unexpected table present( in Hive metastore)?: %s.test_system_table\\$partitions".formatted(schemaName));
                assertThat(computeActual("SHOW SCHEMAS").getOnlyColumnAsSet()).contains(schemaName);
                assertThat(computeActual("SHOW TABLES IN " + schemaName).getOnlyColumnAsSet())
                        .contains("test_system_table$partitions");

                dropTableFromMetastore(schemaName, "test_system_table$partitions");
                dropDatabaseFromMetastore(schemaName);
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    @Override
    public void testShowCreateSchema()
    {
        assertThat(computeScalar("SHOW CREATE SCHEMA tpch")).isEqualTo("" +
                "CREATE SCHEMA objectstore.tpch\n" +
                (isGalaxyMetastore ? "AUTHORIZATION ROLE \"accountadmin\"\n" : "") +
                "WITH (\n" +
                "   location = 's3://" + bucketName + "/tpch'\n" +
                ")");
    }

    @Test
    @Override
    public void testShowCreateInformationSchema()
    {
        if (!isGalaxyMetastore) {
            super.testShowCreateInformationSchema();
            return;
        }

        assertThat(computeScalar("SHOW CREATE SCHEMA information_schema"))
                .isEqualTo(format("CREATE SCHEMA %s.information_schema\nAUTHORIZATION ROLE accountadmin", getSession().getCatalog().orElseThrow()));
    }

    @Test
    public void testTableTypesAndFormats()
    {
        assertHiveTableFormat("PARQUET");
        assertHiveTableFormat("ORC");
        assertHiveTableFormat("TEXTFILE");

        assertIcebergTableFormat("PARQUET");
        assertIcebergTableFormat("ORC");

        assertDeltaTableFormat();

        assertHudiTableFormat();
    }

    private static String locationUuidRegex()
    {
        return "-[0-9a-f]{32}";
    }

    private void assertHiveTableFormat(String format)
    {
        // Use same table name in all assert*TableFormat methods to verify potential metastore caching doesn't affect table loading after table type changed
        @Language("SQL") String createTable = format("" +
                "CREATE TABLE objectstore.tpch.test_type_format (\n" +
                "   abc bigint\n" +
                ")\n" +
                "WITH (\n" +
                "   format = '%s',\n" +
                "   type = 'HIVE'\n" +
                ")", format);

        assertUpdate(createTable);

        assertThat(computeScalar("SHOW CREATE TABLE test_type_format")).isEqualTo(createTable);

        assertUpdate("DROP TABLE test_type_format");
    }

    private void assertIcebergTableFormat(String format)
    {
        // Use same table name in all assert*TableFormat methods to verify potential metastore caching doesn't affect table loading after table type changed
        assertUpdate(format("" +
                "CREATE TABLE objectstore.tpch.test_type_format (\n" +
                "   abc bigint\n" +
                ")\n" +
                "WITH (\n" +
                "   format = '%s',\n" +
                "   type = 'ICEBERG'\n" +
                ")", format));

        assertThat((String) computeActual("SHOW CREATE TABLE test_type_format").getOnlyValue()).matches("" +
                "\\QCREATE TABLE objectstore.tpch.test_type_format (\n" +
                "   abc bigint\n" +
                ")\n" +
                "WITH (\n" +
                "   format = '" + format + "',\n" +
                "   format_version = 3,\n" +
                "   location = 's3://test-bucket-\\E\\w+\\Q/tpch/test_type_format\\E" + locationUuidRegex() + "\\Q',\n" +
                "   type = 'ICEBERG'\n" +
                ")\\E");

        assertUpdate("DROP TABLE test_type_format");
    }

    private void assertDeltaTableFormat()
    {
        // Use same table name in all assert*TableFormat methods to verify potential metastore caching doesn't affect table loading after table type changed
        assertUpdate(format("" +
                "CREATE TABLE objectstore.tpch.test_type_format (\n" +
                "   abc bigint\n" +
                ")\n" +
                "WITH (\n" +
                "   type = 'DELTA'\n" +
                ")"));

        assertThat((String) computeActual("SHOW CREATE TABLE test_type_format").getOnlyValue()).matches("" +
                "\\QCREATE TABLE objectstore.tpch.test_type_format (\n" +
                "   abc bigint\n" +
                ")\n" +
                "WITH (\n" +
                "   location = 's3://test-bucket-\\E\\w+\\Q/tpch/test_type_format\\E" + locationUuidRegex() + "\\Q',\n" +
                "   type = 'DELTA'\n" +
                ")\\E");

        assertUpdate("DROP TABLE test_type_format");
    }

    private void assertHudiTableFormat()
    {
        // Use same table name in all assert*TableFormat methods to verify potential metastore caching doesn't affect table loading after table type changed
        assertQueryFails(
                "CREATE TABLE test_type_format (abc bigint) WITH (type = 'HUDI')\n",
                "Table creation is not supported for Hudi");
    }

    @Test
    public void testHiveSpecificTableProperty()
    {
        assertUpdate("" +
                "CREATE TABLE test_hive_specific_property(\n" +
                "   abc bigint\n" +
                ")\n" +
                "WITH (\n" +
                "   auto_purge = true\n" +
                ")");

        assertThat(computeScalar("SHOW CREATE TABLE test_hive_specific_property")).isEqualTo("" +
                "CREATE TABLE objectstore.tpch.test_hive_specific_property (\n" +
                "   abc bigint\n" +
                ")\n" +
                "WITH (\n" +
                "   auto_purge = true,\n" +
                "   format = 'ORC',\n" +
                "   type = 'HIVE'\n" +
                ")");
    }

    @Test
    public void testHiveSpecificColumnProperty()
    {
        assertUpdate("" +
                "CREATE TABLE test_hive_specific_column_property(\n" +
                "   xyz bigint,\n" +
                "   abc bigint\n" +
                "     WITH (partition_projection_type = 'INTEGER', partition_projection_range = ARRAY['0', '10'])\n" +
                ") WITH (" +
                "    partitioned_by = ARRAY['abc'],\n" +
                "    partition_projection_enabled = true,\n" +
                "    partition_projection_location_template = 's3://example/${abc}'\n" +
                ")");

        assertThat(computeScalar("SHOW CREATE TABLE test_hive_specific_column_property")).isEqualTo("" +
                "CREATE TABLE objectstore.tpch.test_hive_specific_column_property (\n" +
                "   xyz bigint,\n" +
                "   abc bigint WITH (partition_projection_range = ARRAY['0','10'], partition_projection_type = 'INTEGER')\n" +
                ")\n" +
                "WITH (\n" +
                "   format = 'ORC',\n" +
                "   partition_projection_enabled = true,\n" +
                "   partition_projection_location_template = 's3://example/${abc}',\n" +
                "   partitioned_by = ARRAY['abc'],\n" +
                "   type = 'HIVE'\n" +
                ")");
    }

    @Test
    public void testIcebergSpecificTableProperty()
    {
        assertUpdate("" +
                "CREATE TABLE test_iceberg_specific_property(\n" +
                "   abc bigint,\n" +
                "   xyz varchar\n" +
                ")\n" +
                "WITH (\n" +
                "   partitioning = ARRAY['bucket(abc, 13)']\n" +
                ")");

        assertThat((String) computeActual("SHOW CREATE TABLE test_iceberg_specific_property").getOnlyValue()).matches("" +
                "\\QCREATE TABLE objectstore.tpch.test_iceberg_specific_property (\n" +
                "   abc bigint,\n" +
                "   xyz varchar\n" +
                ")\n" +
                "WITH (\n" +
                "   format = 'PARQUET',\n" +
                "   format_version = 3,\n" +
                "   location = 's3://test-bucket-\\E\\w+\\Q/tpch/test_iceberg_specific_property\\E" + locationUuidRegex() + "\\Q',\n" +
                "   partitioning = ARRAY['bucket(abc, 13)'],\n" +
                "   type = 'ICEBERG'\n" +
                ")\\E");
    }

    @Test
    public void testDeltaSpecificTableProperty()
    {
        assertUpdate("" +
                "CREATE TABLE test_delta_specific_property(\n" +
                "   abc bigint\n" +
                ")\n" +
                "WITH (\n" +
                "   checkpoint_interval = 42\n" +
                ")");

        assertThat((String) computeActual("SHOW CREATE TABLE test_delta_specific_property").getOnlyValue()).matches("" +
                "\\QCREATE TABLE objectstore.tpch.test_delta_specific_property (\n" +
                "   abc bigint\n" +
                ")\n" +
                "WITH (\n" +
                "   checkpoint_interval = 42,\n" +
                "   location = 's3://test-bucket-\\E\\w+\\Q/tpch/test_delta_specific_property\\E" + locationUuidRegex() + "\\Q',\n" +
                "   type = 'DELTA'\n" +
                ")\\E");
    }

    @Test
    public void testCreateOrReplaceTableDoesNotChangeTableType()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_OR_REPLACE_TABLE));
        assertUpdate("CREATE OR REPLACE TABLE test_create_or_replace_table_type AS SELECT 1 AS abc", 1);
        switch (tableType) {
            case HIVE, HUDI -> abort(format("CREATE OR REPLACE TABLE queries are not supporred for the table type: %s", tableType));
            case ICEBERG -> {
                assertQueryFails(
                        "CREATE OR REPLACE TABLE test_create_or_replace_table_type (abc integer) WITH (type = 'DELTA')",
                        "tpch.test_create_or_replace_table_type is not a Delta Lake table");
                assertQueryFails(
                        "CREATE OR REPLACE TABLE test_create_or_replace_table_type WITH (type = 'DELTA') AS SELECT 2 AS abc",
                        "tpch.test_create_or_replace_table_type is not a Delta Lake table");
            }
            case DELTA -> {
                assertQueryFails(
                        "CREATE OR REPLACE TABLE test_create_or_replace_table_type (abc integer) WITH (type = 'ICEBERG')",
                        "Not an Iceberg table: tpch.test_create_or_replace_table_type");
                assertQueryFails(
                        "CREATE OR REPLACE TABLE test_create_or_replace_table_type WITH (type = 'ICEBERG') AS SELECT 2 AS abc",
                        "Not an Iceberg table: tpch.test_create_or_replace_table_type");
            }
        }
        assertQuery("SELECT * FROM test_create_or_replace_table_type", "VALUES 1");
    }

    @Test
    public void testMigrateToIcebergTable()
    {
        assertUpdate("CREATE TABLE test_migrate_to_iceberg AS SELECT 1 AS abc", 1);
        switch (tableType) {
            case HIVE -> {
                assertUpdate("ALTER TABLE test_migrate_to_iceberg SET PROPERTIES type = 'ICEBERG'");
                assertThat((String) computeScalar("SHOW CREATE TABLE test_migrate_to_iceberg"))
                        .contains("type = 'ICEBERG'");
                assertQuery("SELECT * FROM test_migrate_to_iceberg", "VALUES 1");
            }
            case ICEBERG, DELTA, HUDI -> assertQueryFails(
                    "ALTER TABLE test_migrate_to_iceberg SET PROPERTIES type = 'ICEBERG'",
                    "Changing table type from '%s' to 'ICEBERG' is not supported".formatted(tableType));
        }
    }

    @Test
    public void testMigrateToDeltaTable()
    {
        assertUpdate("CREATE TABLE test_migrate_to_delta AS SELECT 1 AS abc", 1);
        assertQueryFails(
                "ALTER TABLE test_migrate_to_delta SET PROPERTIES type = 'DELTA'",
                "Changing table type from '%s' to 'DELTA' is not supported".formatted(tableType));
    }

    @Test
    public void testMigrateToHiveTable()
    {
        assertUpdate("CREATE TABLE test_migrate_to_hive AS SELECT 1 AS abc", 1);
        assertQueryFails(
                "ALTER TABLE test_migrate_to_hive SET PROPERTIES type = 'HIVE'",
                "Changing table type from '%s' to 'HIVE' is not supported".formatted(tableType));
    }

    @Test
    public void testMigrateToHudiTable()
    {
        assertUpdate("CREATE TABLE test_migrate_to_hudi AS SELECT 1 AS abc", 1);
        assertQueryFails(
                "ALTER TABLE test_migrate_to_hudi SET PROPERTIES type = 'HUDI'",
                "Changing table type from '%s' to 'HUDI' is not supported".formatted(tableType));
    }

    @Test
    void testAddFilesFromTableToIcebergTable()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE));

        assertUpdate("CREATE TABLE test_source_add_files_from_table_to_iceberg AS SELECT 1 AS x", 1);
        assertUpdate("CREATE TABLE test_target_add_files_from_table_to_iceberg(x int) WITH (type = 'ICEBERG')");
        switch (tableType) {
            case HIVE -> {
                assertUpdate("ALTER TABLE test_target_add_files_from_table_to_iceberg EXECUTE add_files_from_table('tpch', 'test_source_add_files_from_table_to_iceberg')");
                assertQuery("SELECT * FROM test_target_add_files_from_table_to_iceberg", "VALUES 1");
            }
            case ICEBERG, DELTA, HUDI -> assertQueryFails(
                    "ALTER TABLE test_target_add_files_from_table_to_iceberg EXECUTE add_files_from_table('tpch', 'test_source_add_files_from_table_to_iceberg')",
                    "Adding files from non-Hive tables is unsupported");
        }
        assertUpdate("DROP TABLE test_source_add_files_from_table_to_iceberg");
        assertUpdate("DROP TABLE test_target_add_files_from_table_to_iceberg");
    }

    @Test
    void testAddFilesToIcebergTable()
    {
        assertUpdate("CREATE TABLE test_source_add_files_to_iceberg WITH (type = 'HIVE', format = 'ORC') AS SELECT 1 AS x", 1);
        assertUpdate("CREATE TABLE test_target_add_files_to_iceberg(x int) WITH (type = 'ICEBERG')");

        String path = (String) computeScalar("SELECT \"$path\" FROM test_source_add_files_to_iceberg");

        assertUpdate("ALTER TABLE test_target_add_files_to_iceberg EXECUTE add_files('" + path + "', 'ORC')");
        assertQuery("SELECT * FROM test_target_add_files_to_iceberg", "VALUES 1");

        assertUpdate("DROP TABLE test_source_add_files_to_iceberg");
        assertUpdate("DROP TABLE test_target_add_files_to_iceberg");
    }

    @Test
    void testAddFilesFromDeniedLocation()
    {
        if (!isGalaxyMetastore) {
            return;
        }

        String location = "s3://%s/denied".formatted(bucketName);
        assertUpdate("CREATE TABLE test_target_add_files_denied_location(x int) WITH (type = 'ICEBERG')");

        assertQueryFails("ALTER TABLE test_target_add_files_denied_location EXECUTE add_files('" + location + "', 'ORC')",
                "Access Denied: Role accountadmin is not allowed to use location: " + location);

        assertUpdate("DROP TABLE test_target_add_files_denied_location");
    }

    @Test
    public void testCreateSchemaWithLocation()
    {
        if (!isGalaxyMetastore) {
            return;
        }

        String location = "s3://%s/denied".formatted(bucketName);
        assertQueryFails("CREATE SCHEMA test_location_create WITH (location = '" + location + "')",
                "Access Denied: Role accountadmin is not allowed to use location: " + location);
    }

    @Test
    public void testCreateTableWithLocation()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE));
        skipTestUnless(isGalaxyMetastore);
        String location = "s3://%s/denied".formatted(bucketName);

        assertQueryFails("CREATE TABLE test_location_create (x int) WITH (location = '" + location + "')",
                "Access Denied: Role accountadmin is not allowed to use location: " + location);

        assertQueryFails("CREATE TABLE test_location_create (x int) WITH (location = '" + location + "/test_location_create')",
                "Access Denied: Role accountadmin is not allowed to use location: " + location + "/test_location_create");
    }

    @Test
    public void testCreateTableAsWithLocation()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE));
        skipTestUnless(isGalaxyMetastore);
        String location = "s3://%s/denied".formatted(bucketName);

        assertQueryFails("CREATE TABLE test_location_ctas WITH (location = '" + location + "') AS SELECT 123 x",
                "Access Denied: Role accountadmin is not allowed to use location: " + location);

        assertQueryFails("CREATE TABLE test_location_ctas WITH (location = '" + location + "/test_location_ctas') AS SELECT 123 x",
                "Access Denied: Role accountadmin is not allowed to use location: " + location + "/test_location_ctas");
    }

    @Test
    public void testBasicTableStatistics()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE));

        assertUpdate("CREATE TABLE test_basic_table_statistics (x BIGINT)");
        assertUpdate("INSERT INTO test_basic_table_statistics VALUES -42", 1);
        assertUpdate("INSERT INTO test_basic_table_statistics VALUES 88", 1);

        // SHOW STATS result: column_name, data_size, distinct_values_count, nulls_fractions, row_count, low_value, high_value

        assertThat(computeActual("SHOW STATS FOR test_basic_table_statistics"))
                .isEqualTo(resultBuilder(getSession(), VARCHAR, DOUBLE, DOUBLE, DOUBLE, DOUBLE, VARCHAR, VARCHAR)
                        .row("x", null, basicTableStatisticsExpectedNdv(2), 0.0, null, "-42", "88")
                        .row(null, null, null, null, 2.0, null, null)
                        .build());

        assertUpdate("INSERT INTO test_basic_table_statistics VALUES 222", 1);

        assertThat(computeActual("SHOW STATS FOR test_basic_table_statistics"))
                .isEqualTo(resultBuilder(getSession(), VARCHAR, DOUBLE, DOUBLE, DOUBLE, DOUBLE, VARCHAR, VARCHAR)
                        .row("x", null, basicTableStatisticsExpectedNdv(3), 0.0, null, "-42", "222")
                        .row(null, null, null, null, 3.0, null, null)
                        .build());
    }

    protected Double basicTableStatisticsExpectedNdv(int actualNdv)
    {
        return null;
    }

    @Test
    public void testAnalyzePropertiesSystemTable()
    {
        // Note, this is a union of all the analyze table properties across iceberg/delta/hive
        // for example: iceberg does not support analyze at all and only delta supports files_modified_after
        assertQuery(
                "SELECT * FROM system.metadata.analyze_properties WHERE catalog_name = 'objectstore'",
                "SELECT * FROM VALUES " +
                        "('objectstore', 'mode', 'INCREMENTAL', 'varchar', 'Analyze mode. Possible values: [INCREMENTAL, FULL_REFRESH]'), " +
                        "('objectstore', 'partitions', '', 'array(array(varchar))', 'Partitions to be analyzed'), " +
                        "('objectstore', 'columns', '', 'array(varchar)', 'Columns to be analyzed'), " +
                        "('objectstore', 'files_modified_after', '' , 'timestamp(3) with time zone', 'Take into account only files modified after given timestamp') ");
    }

    @Test
    public void testRegisterTableProcedure()
            throws Exception
    {
        String tableName = "test_register_table_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 1 x", 1);

        String tableLocation = getTableLocation(tableName);
        dropTableFromMetastore("tpch", tableName);

        assertUpdate("CALL system.flush_metadata_cache(SCHEMA_NAME => CURRENT_SCHEMA, TABLE_NAME => '" + tableName + "')");

        assertQueryFails("SELECT * FROM " + tableName, ".*Table '.*' does not exist");

        assertUpdate("CALL system.register_table (CURRENT_SCHEMA, '" + tableName + "', '" + tableLocation + "')");

        assertQuery("SELECT * FROM " + tableName, "VALUES 1");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testRegisterTableProcedureIcebergSpecificArgument()
            throws Exception
    {
        String tableName = "test_register_table_iceberg_specific_argument_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 1 x", 1);

        String tableLocation = getTableLocation(tableName);
        dropTableFromMetastore("tpch", tableName);
        assertUpdate("CALL system.flush_metadata_cache(SCHEMA_NAME => CURRENT_SCHEMA, TABLE_NAME => '" + tableName + "')");

        switch (tableType) {
            case ICEBERG -> {
                String key = tableLocation.substring(minio.getS3Url().length() + 1) + "/metadata/";
                String metadataFileName = minio.listObjects(key).stream()
                        .filter(path -> path.endsWith(".json"))
                        .map(path -> Path.of(path).getFileName().toString())
                        .max(Comparator.comparing((String fileName) -> {
                            // e.g. "00001-dd701085-154b-4ca3-af16-5a4359ffbdf5.metadata.json"
                            Matcher matcher = Pattern.compile("(\\d{5})(-[0-9a-f]+){5}\\.metadata\\.json").matcher(fileName);
                            verify(matcher.matches(), "no match for [%s] in [%s]", matcher.pattern().pattern(), fileName);
                            return parseInt(matcher.group(1));
                        }))
                        .orElseThrow();
                assertUpdate("CALL system.register_table (CURRENT_SCHEMA, '" + tableName + "', '" + tableLocation + "', '" + metadataFileName + "')");
                assertQuery("SELECT * FROM " + tableName, "VALUES 1");
            }
            case DELTA -> {
                assertQueryFails(
                        "CALL system.register_table (CURRENT_SCHEMA, '" + tableName + "', '" + tableLocation + "', 'dummy metadata_file_name argument')",
                        "Unsupported metadata_file_name argument.*");
                assertQueryFails("SELECT * FROM " + tableName, ".*Table '.*' does not exist");
            }
            case HUDI, HIVE -> {
                assertQueryFails(
                        "CALL system.register_table (CURRENT_SCHEMA, '" + tableName + "', '" + tableLocation + "', 'dummy metadata_file_name argument')",
                        "Unsupported table type");
                assertQueryFails("SELECT * FROM " + tableName, ".*Table '.*' does not exist");
            }
        }

        assertUpdate("DROP TABLE IF EXISTS " + tableName);
    }

    @Test
    public void testRegisterTableTypesFailure()
    {
        String tableName = "test_register_table_types_" + randomNameSuffix();

        String tableLocation = "s3://%s/%s".formatted(bucketName, tableName);

        minio.putObject(tableName + "/metadata/dummy_metadata.json", "dummy");
        minio.putObject(tableName + "/_delta_log/dummy_transaction.json", "dummy");
        minio.putObject(tableName + "/.hoodie/dummy.commit", "dummy");

        assertQueryFails(
                "CALL system.register_table (CURRENT_SCHEMA, '" + tableName + "', '" + tableLocation + "')",
                "Cannot determine any one of Iceberg, Delta Lake, Hudi table types");
        assertQueryFails("SELECT * FROM " + tableName, ".*Table '.*' does not exist");
    }

    @Test
    public void testRegisterTableAccessControl()
    {
        if (!isGalaxyMetastore) {
            return;
        }

        String tableName = "test_register_table_" + randomNameSuffix();
        String location = "s3://%s/denied".formatted(bucketName);
        assertQueryFails("CALL system.register_table (CURRENT_SCHEMA, '" + tableName + "', '" + location + "')",
                "Access Denied: Role accountadmin is not allowed to use location: " + location);
    }

    @Test
    public void testUnregisterTableProcedure()
    {
        String tableName = "test_unregister_table_" + randomNameSuffix();
        String unregisterTableName = tableName + "_new";

        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 1 x", 1);

        String tableLocation = getTableLocation(tableName);

        assertUpdate("CALL system.register_table(CURRENT_SCHEMA, '" + unregisterTableName + "', '" + tableLocation + "')");
        assertThat(getQueryRunner().tableExists(getSession(), unregisterTableName)).isTrue();

        assertUpdate("CALL system.unregister_table(CURRENT_SCHEMA, '" + unregisterTableName + "')");
        assertThat(getQueryRunner().tableExists(getSession(), unregisterTableName)).isFalse();

        assertQuery("SELECT * FROM " + tableName, "VALUES 1");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testUnregisterTableAccessControl()
    {
        String tableName = "test_unregister_table_access_control_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 1 a", 1);

        assertAccessDenied(
                "CALL system.unregister_table(CURRENT_SCHEMA, '" + tableName + "')",
                "Cannot drop table .*",
                privilege(tableName, DROP_TABLE));

        assertQuery("SELECT * FROM " + tableName, "VALUES 1");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testDropTableCorruptStorage()
    {
        String tableName = "corrupt_table_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + " (name VARCHAR(256), age INTEGER)");
        assertUpdate("INSERT INTO " + tableName + " VALUES ('Joe', 30)", 1);

        String tableLocation = getTableLocation(tableName);
        String tableLocationKey = tableLocation.replaceFirst(minio.getS3Url() + "/", "");

        // break the table by deleting all its files including metadata files
        List<String> keys = minio.listObjects(tableLocationKey);
        minio.deleteObjects(keys);

        // try to drop table
        assertUpdate("DROP TABLE " + tableName);
        assertThat(getQueryRunner().tableExists(getSession(), tableName)).isFalse();
    }

    protected String getTableLocation(String tableName)
    {
        Pattern locationPattern = Pattern.compile(".*location = '(.*?)'.*", Pattern.DOTALL);
        Matcher matcher = locationPattern.matcher((String) computeActual("SHOW CREATE TABLE " + tableName).getOnlyValue());
        if (matcher.find()) {
            String location = matcher.group(1);
            verify(!matcher.find(), "Unexpected second match");
            return location;
        }
        throw new IllegalStateException("Location not found in SHOW CREATE TABLE result");
    }

    @Test
    @Override // ObjectStore supports this partially, so has non-standard error message
    public void testSetColumnType()
    {
        if (hasBehavior(SUPPORTS_SET_COLUMN_TYPE)) {
            super.testSetColumnType();
        }
        else {
            assertThatThrownBy(super::testSetColumnType)
                    .hasMessageMatching(
                            """

                            Expecting message:
                              "Setting column type on .{4,10} tables is not supported"
                            to match regex:
                              "This connector does not support setting column types"
                            but did not.
                            (?s:.*)""");
        }
    }

    @Test
    @Override // ObjectStore supports this partially, so has non-standard error message
    public void testAddRowField()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE_WITH_DATA) && hasBehavior(SUPPORTS_ROW_TYPE));

        if (hasBehavior(SUPPORTS_ADD_FIELD)) {
            super.testAddRowField();
        }
        else {
            assertThatThrownBy(super::testAddRowField)
                    .hasMessageMatching(
                            """

                            Expecting message:
                              "Adding fields to .{4,10} tables is not supported"
                            to match regex:
                              "This connector does not support adding fields"
                            but did not.
                            (?s:.*)""");
        }
    }

    @Test
    public void testFlushMetadataCache()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE));

        assertUpdate("CREATE TABLE test_flush_metadata_cache(a integer)");
        assertUpdate("CALL system.flush_metadata_cache(schema_name => CURRENT_SCHEMA, table_name => 'test_flush_metadata_cache')");
        assertUpdate("DROP TABLE test_flush_metadata_cache");

        assertUpdate("CALL system.flush_metadata_cache(schema_name => 'flush_metadata_cache_bogus_schema', table_name => 'flush_metadata_cache_non_existent')");
    }

    @Test
    @Override
    public void testCreateViewSchemaNotFound()
    {
        if (!hasBehavior(SUPPORTS_CREATE_VIEW)) {
            super.testCreateViewSchemaNotFound();
            return;
        }
        if (!isGalaxyMetastore) {
            super.testCreateViewSchemaNotFound();
            return;
        }

        // GalaxyAccessControl.checkCanCreateView maybe should throw "Schema xxxx not found"?
        assertThatThrownBy(super::testCreateViewSchemaNotFound)
                .isInstanceOf(AssertionError.class)
                .hasMessageFindingMatch(
                        """

                        Expecting message:
                          "Access Denied: Cannot create view objectstore.test_schema_\\S*.test_view_create_no_schema_\\S*: Role accountadmin does not have the privilege CREATE_TABLE on the schema objectstore.test_schema_\\S*"
                        to match regex:
                          "Schema test_schema_\\S* not found"
                        but did not.
                        """)
                .hasStackTraceContaining("at io.trino.server.security.galaxy.GalaxyAccessControl.checkCanCreateView");
    }

    @Test
    @Override
    public void testCreateTableSchemaNotFound()
    {
        if (!hasBehavior(SUPPORTS_CREATE_TABLE)) {
            super.testCreateTableAsSelectSchemaNotFound();
            return;
        }

        if (!isGalaxyMetastore) {
            super.testCreateTableAsSelectSchemaNotFound();
            return;
        }

        // GalaxyAccessControl.checkCanCreateTable maybe should throw "Schema xxxx not found"?
        assertThatThrownBy(super::testCreateTableSchemaNotFound)
                .isInstanceOf(AssertionError.class)
                .hasMessageFindingMatch(
                        """

                        Expecting message:
                          "Access Denied: Cannot create table objectstore.test_schema_\\S*.test_create_no_schema_\\S*: Role accountadmin does not have the privilege CREATE_TABLE on the schema objectstore.test_schema_\\S*"
                        to match regex:
                          "Schema test_schema_\\S* not found"
                        but did not.
                        """)
                .hasStackTraceContaining("at io.trino.server.security.galaxy.GalaxyAccessControl.checkCanCreateTable");
    }

    @Test
    @Override
    public void testCreateTableAsSelectSchemaNotFound()
    {
        if (!hasBehavior(SUPPORTS_CREATE_TABLE_WITH_DATA)) {
            super.testCreateTableAsSelectSchemaNotFound();
            return;
        }

        if (!isGalaxyMetastore) {
            super.testCreateTableAsSelectSchemaNotFound();
            return;
        }

        // GalaxyAccessControl.checkCanCreateTable maybe should throw "Schema xxxx not found"?
        assertThatThrownBy(super::testCreateTableAsSelectSchemaNotFound)
                .isInstanceOf(AssertionError.class)
                .hasMessageFindingMatch(
                        """

                        Expecting message:
                          "Access Denied: Cannot create table objectstore.test_schema_\\S*.test_ctas_no_schema_\\S*: Role accountadmin does not have the privilege CREATE_TABLE on the schema objectstore.test_schema_\\S*"
                        to match regex:
                          "Schema test_schema_\\S* not found"
                        but did not.
                        """)
                .hasStackTraceContaining("at io.trino.server.security.galaxy.GalaxyAccessControl.checkCanCreateTable");
    }

    @Test
    @Override
    public void testSelectInTransaction()
    {
        // Select in transaction not supported in galaxy
    }

    @Test
    public void testUnloadTableFunction()
    {
        String tableName = "test_unload" + randomNameSuffix();
        String location = "s3://%s/%s".formatted(bucketName, tableName);

        MaterializedResult result = computeActual("SELECT * FROM TABLE(system.unload(" +
                "input => TABLE(VALUES 'test unload') t(col)," +
                "location => '" + location + "'," +
                "format => 'TEXTFILE'," +
                "compression => 'GZIP'," +
                "separator => '#'))");
        assertThat(result.getColumnNames()).containsExactly("path", "count");
        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat((String) getOnlyElement(result.getMaterializedRows()).getField(0)).startsWith(location);

        assertUpdate("CREATE TABLE " + tableName + "(col varchar)" +
                "WITH (type = 'HIVE', format = 'TEXTFILE', textfile_field_separator = '#', external_location = '" + location + "')");
        assertQuery("SELECT * FROM " + tableName, "VALUES 'test unload'");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testUnloadTableFunctionLocationDenied()
    {
        if (!isGalaxyMetastore) {
            return;
        }

        String tableName = "test_unload_denied" + randomNameSuffix();
        String location = "s3://%s/%s".formatted(bucketName, tableName);

        assertQueryFails(
                "SELECT * FROM TABLE(system.unload(" +
                        "input => TABLE(VALUES 'test unload denied') t(col)," +
                        "location => '" + location + "'," +
                        "format => 'TEXTFILE'))",
                "Access Denied: Role accountadmin is not allowed to use location: " + location);
    }

    @Override
    protected void verifyRefreshMaterializedViewFailureWithoutMultiWriteInTransactionSupport(AbstractThrowableAssert abstractThrowableAssert)
    {
        abstractThrowableAssert.hasMessageContaining("Catalogs already associated with transaction");
    }

    protected TrinoFileSystem getTrinoFileSystem()
    {
        return new S3FileSystemFactory(
                OpenTelemetry.noop(),
                new S3FileSystemConfig()
                        .setAwsAccessKey(ACCESS_KEY)
                        .setAwsSecretKey(SECRET_KEY)
                        .setRegion(REGION)
                        .setPathStyleAccess(true)
                        .setEndpoint(minio.getEndpoint()),
                new S3FileSystemStats())
                .create(SESSION);
    }

    @Override
    protected Session withoutSmallFileThreshold(Session session)
    {
        return Session.builder(session)
                .setCatalogSessionProperty(getSession().getCatalog().orElseThrow(), "parquet_small_file_threshold", "0B")
                .setCatalogSessionProperty(getSession().getCatalog().orElseThrow(), "orc_tiny_stripe_threshold", "0B")
                .build();
    }
}
