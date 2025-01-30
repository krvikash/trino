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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.filesystem.Location;
import io.trino.metastore.HiveMetastore;
import io.trino.plugin.hive.containers.HiveHadoop;
import io.trino.plugin.hive.metastore.thrift.BridgingHiveMetastore;
import io.trino.plugin.hive.ozone.HiveOzoneS3Gateway;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import io.trino.tpch.TpchTable;
import org.apache.iceberg.FileFormat;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.plugin.hive.TestingThriftHiveMetastoreBuilder.testingThriftHiveMetastoreBuilder;
import static io.trino.plugin.hive.ozone.ApacheOzoneContainer.DEFAULT_REGION;
import static io.trino.plugin.hive.ozone.ApacheOzoneContainer.DUMMY_ACCESS_KEY;
import static io.trino.plugin.hive.ozone.ApacheOzoneContainer.DUMMY_SECRET_KEY;
import static io.trino.plugin.iceberg.IcebergTestUtils.checkParquetFileSorting;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.QueryAssertions.copyTpchTables;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tpch.TpchTable.LINE_ITEM;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static org.assertj.core.api.Assertions.assertThat;

final class TestIcebergOzoneS3GatewayConnectorSmokeTest
        extends BaseIcebergConnectorSmokeTest
{
    private final String schemaName;
    private final String bucketName;

    private HiveMetastore metastore;
    private HiveOzoneS3Gateway hiveOzoneS3Gateway;
    private HiveHadoop hiveHadoop;

    public TestIcebergOzoneS3GatewayConnectorSmokeTest()
    {
        super(FileFormat.PARQUET);
        this.schemaName = "ozone_" + format.name().toLowerCase(ENGLISH);
        this.bucketName = "test-iceberg-ozone-smoke-test-" + randomNameSuffix();
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        this.hiveOzoneS3Gateway = closeAfterClass(new HiveOzoneS3Gateway(bucketName));
        this.hiveHadoop = hiveOzoneS3Gateway.getHiveHadoop();

        DistributedQueryRunner queryRunner = IcebergQueryRunner.builder(schemaName)
                .setIcebergProperties(
                        ImmutableMap.<String, String>builder()
                                .put("iceberg.file-format", format.name())
                                .put("iceberg.max-format-version", "3")
                                .put("iceberg.catalog.type", "HIVE_METASTORE")
                                .put("hive.metastore.uri", hiveHadoop.getHiveMetastoreEndpoint().toString())
                                .put("hive.metastore.thrift.client.read-timeout", "1m") // read timed out sometimes happens with the default timeout
                                .put("iceberg.register-table-procedure.enabled", "true")
                                .put("iceberg.writer-sort-buffer-size", "1MB")
                                .put("fs.hadoop.enabled", "false")
                                .put("fs.s3.enabled", "true")
                                .put("s3.aws-access-key", DUMMY_ACCESS_KEY)
                                .put("s3.aws-secret-key", DUMMY_SECRET_KEY)
                                .put("s3.region", DEFAULT_REGION)
                                .put("s3.endpoint", hiveOzoneS3Gateway.getApacheOzoneContainer().getS3EndpointAddress())
                                .put("s3.path-style-access", "true")
                                .put("s3.streaming.part-size", "5MB") // minimize memory usage
                                .put("s3.max-connections", "2") // verify no leaks
                                .put("iceberg.allowed-extra-properties", "write.metadata.delete-after-commit.enabled,write.metadata.previous-versions-max")
                                .buildOrThrow())
                .build();

        queryRunner.execute("CREATE SCHEMA " + schemaName + " WITH (location = 's3://" + bucketName + "/" + schemaName + "')");
        copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, queryRunner.getDefaultSession(), ImmutableList.<TpchTable<?>>builder()
                .addAll(REQUIRED_TPCH_TABLES)
                .add(LINE_ITEM)
                .build());

        metastore = new BridgingHiveMetastore(
                testingThriftHiveMetastoreBuilder()
                        .metastoreClient(hiveHadoop.getHiveMetastoreEndpoint())
                        .build(this::closeAfterClass));
        return queryRunner;
    }

    @AfterAll
    public void destroy()
    {
        hiveOzoneS3Gateway = null; // closed by closeAfterClass
    }

    @Override
    protected String createSchemaSql(String schemaName)
    {
        return "CREATE SCHEMA IF NOT EXISTS " + schemaName + " WITH (location = 's3://" + bucketName + "/" + schemaName + "')";
    }

    @Test
    @Override
    public void testRenameSchema()
    {
        assertQueryFails(
                format("ALTER SCHEMA %s RENAME TO %s", schemaName, schemaName + randomNameSuffix()),
                "Hive metastore does not support renaming schemas");
    }

    @Test
    public void testS3LocationWithTrailingSlash()
    {
        String tableName = "test_s3_location_with_trailing_slash_" + randomNameSuffix();
        String location = "s3://%s/%s/%s/".formatted(bucketName, schemaName, tableName);
        // Verify data and metadata files' uri don't contain fragments
        assertThat(location).doesNotContain("#");

        assertUpdate("CREATE TABLE " + tableName + " WITH (location='" + location + "') AS SELECT 1 col", 1);

        List<String> dataFiles = hiveOzoneS3Gateway.listFiles("%s/%s/data".formatted(schemaName, tableName));
        assertThat(dataFiles).isNotEmpty().filteredOn(filePath -> filePath.contains("#")).isEmpty();

        List<String> metadataFiles = hiveOzoneS3Gateway.listFiles("%s/%s/metadata".formatted(schemaName, tableName));
        assertThat(metadataFiles).isNotEmpty().filteredOn(filePath -> filePath.contains("#")).isEmpty();

        // Verify ALTER TABLE succeeds https://github.com/trinodb/trino/issues/14552
        assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN new_col int");
        assertTableColumnNames(tableName, "col", "new_col");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testMetadataLocationWithDoubleSlash()
    {
        // Regression test for https://github.com/trinodb/trino/issues/14299
        try (TestTable testTable = new TestTable(
                getQueryRunner()::execute,
                "test_meatdata_location_with_double_slash_",
                " AS SELECT 1 col")) {
            // Update metadata location to contain double slash
            String tableId = onMetastore("SELECT tbl_id FROM TBLS t INNER JOIN DBS db ON t.db_id = db.db_id WHERE db.name = '" + schemaName + "' and t.tbl_name = '" + testTable.getName() + "'");
            String metadataLocation = onMetastore("SELECT param_value FROM TABLE_PARAMS WHERE param_key = 'metadata_location' AND tbl_id = " + tableId);

            // Simulate corrupted metadata location as Trino 393-394 was doing
            String newMetadataLocation = metadataLocation.replace("/metadata/", "//metadata/");
            onMetastore("UPDATE TABLE_PARAMS SET param_value = '" + newMetadataLocation + "' WHERE tbl_id = " + tableId + " AND param_key = 'metadata_location'");

            // Confirm read and write operations succeed
            assertQuery("SELECT * FROM " + testTable.getName(), "VALUES 1");
            assertUpdate("INSERT INTO " + testTable.getName() + " VALUES 2", 1);
            assertQuery("SELECT * FROM " + testTable.getName(), "VALUES (1), (2)");
        }
    }

    private String onMetastore(String sql)
    {
        return hiveHadoop.executeInContainer(sql).getStdout();
    }

    @Override
    protected void dropTableFromCatalog(String tableName)
    {
        metastore.dropTable(getSession().getSchema().orElseThrow(), tableName, false);
        assertThat(metastore.getTable(getSession().getSchema().orElseThrow(), tableName)).as("Table in metastore should be dropped").isEmpty();
    }

    @Override
    protected String getMetadataLocation(String tableName)
    {
        return metastore
                .getTable(schemaName, tableName).orElseThrow()
                .getParameters().get("metadata_location");
    }

    @Override
    protected String schemaPath()
    {
        return format("s3://%s/%s", bucketName, schemaName);
    }

    @Override
    protected boolean locationExists(String location)
    {
        return !hiveOzoneS3Gateway.listFiles(location).isEmpty();
    }

    @Override
    protected void deleteDirectory(String location)
    {
        for (String file : hiveOzoneS3Gateway.listFiles(location)) {
            hiveOzoneS3Gateway.deleteFile(file);
        }
        assertThat(hiveOzoneS3Gateway.listFiles(location)).isEmpty();
    }

    @Override
    protected boolean isFileSorted(Location path, String sortColumnName)
    {
        return checkParquetFileSorting(fileSystem.newInputFile(path), sortColumnName);
    }

    @Override
    protected String getCreateCatalogSqlTemplate()
    {
        return getCreateCatalogSqlTemplate(DUMMY_SECRET_KEY);
    }

    private String getCreateCatalogSqlTemplate(String secretKey)
    {
        return """
               CREATE CATALOG %s USING iceberg
               WITH (
                  "fs.hadoop.enabled" = 'false',
                  "fs.s3.enabled" = 'true',
                  "hive.metastore.uri" = '%s',
                  "iceberg.catalog.type" = 'HIVE_METASTORE',
                  "iceberg.file-format" = '%s',
                  "s3.aws-access-key" = '%s',
                  "s3.aws-secret-key" = '%s',
                  "s3.endpoint" = '%s',
                  "s3.path-style-access" = 'true',
                  "s3.region" = '%s'
               )""".formatted(
                "%1$s", // Catalog name
                hiveHadoop.getHiveMetastoreEndpoint(),
                "%2$s", // Metastore URI
                DUMMY_ACCESS_KEY,
                secretKey,
                hiveOzoneS3Gateway.getApacheOzoneContainer().getS3EndpointAddress(),
                DEFAULT_REGION);
    }
}
