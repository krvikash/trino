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
package io.trino.plugin.iceberg.catalog.glue;

import com.amazonaws.services.glue.AWSGlueAsync;
import com.amazonaws.services.glue.AWSGlueAsyncClientBuilder;
import com.amazonaws.services.glue.model.DeleteTableRequest;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.hive.metastore.glue.GlueMetastoreApiStats;
import io.trino.plugin.iceberg.BaseIcebergConnectorSmokeTest;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.plugin.iceberg.SchemaInitializer;
import io.trino.plugin.iceberg.TestIcebergRegisterTableProcedure;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestView;
import org.apache.iceberg.FileFormat;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.hive.metastore.glue.AwsSdkUtil.getPaginatedResults;
import static io.trino.plugin.iceberg.TestIcebergRegisterTableProcedure.StorageFormat.ORC;
import static io.trino.testing.sql.TestTable.randomTableSuffix;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/*
 * TestIcebergGlueCatalogConnectorSmokeTest currently uses AWS Default Credential Provider Chain,
 * See https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html#credentials-default
 * on ways to set your AWS credentials which will be needed to run this test.
 */
public class TestIcebergGlueCatalogConnectorSmokeTest
        extends BaseIcebergConnectorSmokeTest
{
    private final String bucketName;
    private final String schemaName;
    private final AWSGlueAsync glueClient;
    private static final String ICEBRG_CATALOG = "iceberg";

    @Parameters("s3.bucket")
    public TestIcebergGlueCatalogConnectorSmokeTest(String bucketName)
    {
        super(FileFormat.PARQUET);
        this.bucketName = requireNonNull(bucketName, "bucketName is null");
        this.schemaName = "test_iceberg_smoke_" + randomTableSuffix();
        glueClient = AWSGlueAsyncClientBuilder.defaultClient();
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .setIcebergProperties(
                        ImmutableMap.of(
                                "iceberg.catalog.type", "glue",
                                "hive.metastore.glue.default-warehouse-dir", schemaPath()))
                .setSchemaInitializer(
                        SchemaInitializer.builder()
                                .withClonedTpchTables(REQUIRED_TPCH_TABLES)
                                .withSchemaName(schemaName)
                                .build())
                .build();
    }

    @AfterClass(alwaysRun = true)
    public void cleanup()
    {
        computeActual("SHOW TABLES").getMaterializedRows()
                .forEach(table -> getQueryRunner().execute("DROP TABLE " + table.getField(0)));
        getQueryRunner().execute("DROP SCHEMA IF EXISTS " + schemaName);

        // DROP TABLES should clean up any files, but clear the directory manually to be safe
        AmazonS3 s3 = AmazonS3ClientBuilder.standard().build();

        ListObjectsV2Request listObjectsRequest = new ListObjectsV2Request()
                .withBucketName(bucketName)
                .withPrefix(schemaPath());
        List<DeleteObjectsRequest.KeyVersion> keysToDelete = getPaginatedResults(
                s3::listObjectsV2,
                listObjectsRequest,
                ListObjectsV2Request::setContinuationToken,
                ListObjectsV2Result::getNextContinuationToken,
                new GlueMetastoreApiStats())
                .map(ListObjectsV2Result::getObjectSummaries)
                .flatMap(objectSummaries -> objectSummaries.stream().map(S3ObjectSummary::getKey))
                .map(DeleteObjectsRequest.KeyVersion::new)
                .collect(toImmutableList());

        if (!keysToDelete.isEmpty()) {
            s3.deleteObjects(new DeleteObjectsRequest(bucketName).withKeys(keysToDelete));
        }
    }

    @Test
    @Override
    public void testShowCreateTable()
    {
        assertThat((String) computeScalar("SHOW CREATE TABLE region"))
                .matches(format("" +
                                "\\QCREATE TABLE iceberg.%1$s.region (\n" +
                                "   regionkey bigint,\n" +
                                "   name varchar,\n" +
                                "   comment varchar\n" +
                                ")\n" +
                                "WITH (\n" +
                                "   format = 'ORC',\n" +
                                "   format_version = 2,\n" +
                                "   location = '%2$s/%1$s.db/region-\\E.*\\Q'\n" +
                                ")\\E",
                        schemaName,
                        schemaPath()));
    }

    @Test
    @Override
    public void testRenameSchema()
    {
        assertThatThrownBy(super::testRenameSchema)
                .hasStackTraceContaining("renameNamespace is not supported for Iceberg Glue catalogs");
    }

    @Test
    public void testCommentView()
    {
        // TODO: Consider moving to BaseConnectorSmokeTest
        try (TestView view = new TestView(getQueryRunner()::execute, "test_comment_view", "SELECT * FROM region")) {
            // comment set
            assertUpdate("COMMENT ON VIEW " + view.getName() + " IS 'new comment'");
            assertThat((String) computeScalar("SHOW CREATE VIEW " + view.getName())).contains("COMMENT 'new comment'");
            assertThat(getTableComment(view.getName())).isEqualTo("new comment");

            // comment updated
            assertUpdate("COMMENT ON VIEW " + view.getName() + " IS 'updated comment'");
            assertThat(getTableComment(view.getName())).isEqualTo("updated comment");

            // comment set to empty
            assertUpdate("COMMENT ON VIEW " + view.getName() + " IS ''");
            assertThat(getTableComment(view.getName())).isEmpty();

            // comment deleted
            assertUpdate("COMMENT ON VIEW " + view.getName() + " IS 'a comment'");
            assertThat(getTableComment(view.getName())).isEqualTo("a comment");
            assertUpdate("COMMENT ON VIEW " + view.getName() + " IS NULL");
            assertThat(getTableComment(view.getName())).isNull();
        }
    }

    @Test
    public void testRegisterTableSuccess()
    {
        TestIcebergRegisterTableProcedure.StorageFormat storageFormat = ORC;
        String baseTableName = "test_register_table_with_show_create_table_old_" + storageFormat + "_" + randomTableSuffix();
        String trinoTableName = getTrinoTableName(baseTableName);

        assertUpdate(format("CREATE TABLE %s (a int, b varchar, c boolean) with (format = '%s', partitioning = ARRAY['a'])", trinoTableName, storageFormat));
        assertUpdate(format("COMMENT ON TABLE %s IS '%s'", trinoTableName, "This is table comment"));
        assertUpdate(format("COMMENT ON COLUMN %s.%s IS '%s'", trinoTableName, "b", "This is column comment"));
        assertUpdate(format("INSERT INTO %s values(1, 'INDIA', true)", trinoTableName), 1);
        assertUpdate(format("INSERT INTO %s values(2, 'USA', false)", trinoTableName), 1);
        assertThat(query(format("SELECT * FROM %s", trinoTableName)))
                .matches("VALUES ROW(INT '1', VARCHAR 'INDIA', BOOLEAN 'true'), ROW(INT '2', VARCHAR 'USA', BOOLEAN 'false')");

        String showCreateTableOld = (String) computeActual("SHOW CREATE TABLE " + trinoTableName).getOnlyValue();
        String tableLocation = getTableLocation(trinoTableName);
        // Drop table from glue metastore and use the same table name to register again with the metadata
        DeleteTableRequest deleteTableRequest = new DeleteTableRequest()
                .withDatabaseName(schemaName)
                .withName(baseTableName);
        glueClient.deleteTable(deleteTableRequest);

        assertUpdate(format("CALL iceberg.system.register_table ('%s', '%s', '%s')", schemaName, baseTableName, tableLocation));
        assertUpdate(format("INSERT INTO %s values(3, 'POLAND', true)", trinoTableName), 1);
        assertThat(query(format("SELECT * FROM %s", trinoTableName)))
                .matches("VALUES ROW(INT '1', VARCHAR 'INDIA', BOOLEAN 'true'), ROW(INT '2', VARCHAR 'USA', BOOLEAN 'false')," +
                        " ROW(INT '3', VARCHAR 'POLAND', BOOLEAN 'true')");

        String showCreateTableNew = (String) computeActual("SHOW CREATE TABLE " + trinoTableName).getOnlyValue();

        assertThat(showCreateTableOld).isEqualTo(showCreateTableNew);
        assertUpdate(format("DROP TABLE %s", trinoTableName));
    }

    @Test
    public void testRegisterTableFailure()
    {
        TestIcebergRegisterTableProcedure.StorageFormat storageFormat = TestIcebergRegisterTableProcedure.StorageFormat.ORC;
        String baseTableName = "test_register_table_with_invalid_table_location_and_metadata_file_" + storageFormat + "_" + randomTableSuffix();
        String trinoTableName = getTrinoTableName(baseTableName);

        assertUpdate(format("CREATE TABLE %s (a int, b varchar, c boolean) with (format = '%s')", trinoTableName, storageFormat));
        assertUpdate(format("INSERT INTO %s values(1, 'INDIA', true)", trinoTableName), 1);
        assertUpdate(format("INSERT INTO %s values(2, 'USA', false)", trinoTableName), 1);
        assertThat(query(format("SELECT * FROM %s", trinoTableName)))
                .matches("VALUES ROW(INT '1', VARCHAR 'INDIA', BOOLEAN 'true'), ROW(INT '2', VARCHAR 'USA', BOOLEAN 'false')");

        String tableLocation = getTableLocation(trinoTableName);
        String invalidMetadataFileName = "invalid_metadata_file.json";
        String baseTableNameNew = "test_register_table_with_invalid_table_location_and_metadata_file_new_" + storageFormat + "_" + randomTableSuffix();

        assertQueryFails(format("CALL iceberg.system.register_table ('%s', '%s', '%s', '%s')", schemaName, baseTableNameNew, tableLocation, invalidMetadataFileName),
                ".*Location (.*) does not exist.*");

        // Drop table to verify register_table call fails
        assertUpdate(format("DROP TABLE %s", trinoTableName));

        assertQueryFails(format("CALL iceberg.system.register_table ('%s', '%s', '%s')", schemaName, baseTableNameNew, tableLocation),
                ".*No versioned metadata file exists at location.*");
    }

    @Test
    public void testRegisterTableWithInvalidParameters()
    {
        TestIcebergRegisterTableProcedure.StorageFormat storageFormat = TestIcebergRegisterTableProcedure.StorageFormat.ORC;
        String baseTableName = "test_register_table_with_invalid_parameter_" + storageFormat + "_" + randomTableSuffix();
        String trinoTableName = getTrinoTableName(baseTableName);

        assertUpdate(format("CREATE TABLE %s (a int, b varchar, c boolean) with (format = '%s')", trinoTableName, storageFormat));
        assertUpdate(format("INSERT INTO %s values(1, 'INDIA', true)", trinoTableName), 1);

        String tableLocation = getTableLocation(trinoTableName);
        String baseTableNameNew = "test_register_table_with_invalid_parameter_new_" + storageFormat + "_" + randomTableSuffix();

        assertQueryFails(format("CALL iceberg.system.register_table ('%s', '%s')", schemaName, baseTableNameNew),
                ".*'TABLE_LOCATION' is missing.*");
        assertQueryFails(format("CALL iceberg.system.register_table ('%s', null)", schemaName),
                ".*'TABLE_LOCATION' is missing.*");
        assertQueryFails("CALL iceberg.system.register_table (null, null)",
                ".*'TABLE_LOCATION' is missing.*");
        assertQueryFails(format("CALL iceberg.system.register_table ('%s')", schemaName),
                ".*'TABLE_NAME' is missing.*");
        assertQueryFails("CALL iceberg.system.register_table (null)",
                ".*'TABLE_NAME' is missing.*");
        assertQueryFails("CALL iceberg.system.register_table ()",
                ".*'SCHEMA_NAME' is missing.*");

        assertQueryFails(format("CALL iceberg.system.register_table ('%s', '%s', null)", schemaName, baseTableNameNew),
                ".*Illegal parameter set passed((.|\\n)*)");
        assertQueryFails(format("CALL iceberg.system.register_table ('%s', null, null)", schemaName),
                ".*Illegal parameter set passed((.|\\n)*)");
        assertQueryFails("CALL iceberg.system.register_table (null, null, null)",
                ".*Illegal parameter set passed((.|\\n)*)");

        assertQueryFails(format("CALL iceberg.system.register_table ('%s', '%s', '%s')", "invalid_schema", baseTableNameNew, tableLocation),
                ".*Schema '(.*)' does not exist.*");
        assertQueryFails(format("CALL iceberg.system.register_table ('%s', '%s', '%s')", schemaName, baseTableName, tableLocation),
                ".*Table '(.*)' already exists in schema '(.*)'.*");

        assertUpdate(format("DROP TABLE %s", trinoTableName));
    }

    private String getTableComment(String tableName)
    {
        return (String) computeScalar("SELECT comment FROM system.metadata.table_comments WHERE catalog_name = 'iceberg' AND schema_name = '" + schemaName + "' AND table_name = '" + tableName + "'");
    }

    private String getTrinoTableName(String tableName)
    {
        return format("%s.%s.%s", ICEBRG_CATALOG, schemaName, tableName);
    }

    private String schemaPath()
    {
        return format("s3://%s/%s", bucketName, schemaName);
    }

    private String getTableLocation(String tableName)
    {
        String regex = ".*location = '(.*?)'.*";
        String text = (String) computeActual("SHOW CREATE TABLE " + tableName).getOnlyValue();
        return getRegexMatch(text, regex);
    }

    private String getRegexMatch(String text, String regex)
    {
        Pattern pattern = Pattern.compile(regex, Pattern.DOTALL);
        Matcher m = pattern.matcher(text);
        if (m.find()) {
            String value = m.group(1);
            verify(!m.find(), "Unexpected second match");
            return value;
        }
        throw new IllegalStateException("Pattern not found in the text");
    }
}
