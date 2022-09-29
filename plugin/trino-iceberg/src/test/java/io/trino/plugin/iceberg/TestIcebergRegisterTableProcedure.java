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

import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.hdfs.HdfsFileSystemFactory;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorSession;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static com.google.common.base.Verify.verify;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.trino.plugin.hive.metastore.file.FileHiveMetastore.createTestingFileHiveMetastore;
import static io.trino.plugin.iceberg.IcebergUtil.METADATA_FOLDER_NAME;
import static io.trino.plugin.iceberg.IcebergUtil.getLatestMetadataLocation;
import static io.trino.testing.sql.TestTable.randomTableSuffix;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestIcebergRegisterTableProcedure
        extends AbstractTestQueryFramework
{
    private HiveMetastore metastore;
    private File metastoreDir;
    private TrackingFileSystemFactory trackingFileSystemFactory;
    private TrinoFileSystem trinoFileSystem;
    private static final String TEST_SCHEMA_NAME = "test_schema";
    private static final String CATALOG = "iceberg";

    public enum StorageFormat
    {
        PARQUET("parquet"),
        ORC("orc"),
        AVRO("avro"),
        /**/;
        private final String name;

        StorageFormat(String format)
        {
            this.name = format;
        }

        @Override
        public String toString()
        {
            return this.name;
        }
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        metastoreDir = Files.createTempDirectory("test_iceberg_register_table").toFile();
        metastore = createTestingFileHiveMetastore(metastoreDir);
        trackingFileSystemFactory = new TrackingFileSystemFactory(new HdfsFileSystemFactory(HDFS_ENVIRONMENT));
        trinoFileSystem = trackingFileSystemFactory.create(TestingConnectorSession.SESSION);
        return IcebergQueryRunner.builder()
                .setMetastoreDirectory(metastoreDir)
                .build();
    }

    @BeforeClass
    @Override
    public void init()
            throws Exception
    {
        super.init();
        assertUpdate(format("CREATE SCHEMA %s", TEST_SCHEMA_NAME));
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws IOException
    {
        deleteRecursively(metastoreDir.toPath(), ALLOW_INSECURE);
    }

    @DataProvider
    public static Object[][] storageFormats()
    {
        return Stream.of(StorageFormat.values())
                .map(storageFormat -> new Object[] {storageFormat})
                .toArray(Object[][]::new);
    }

    @Test(dataProvider = "storageFormats")
    public void testRegisterTableWithTableLocation(StorageFormat storageFormat)
    {
        String baseOldTableName = "test_register_table_with_table_location_old_" + randomTableSuffix() + "_" + storageFormat;
        String trinoOldTableName = getTrinoTableName(baseOldTableName);

        assertUpdate(format("CREATE TABLE %s (a int, b varchar, c boolean) with (format = '%s')", trinoOldTableName, storageFormat));
        assertUpdate(format("INSERT INTO %s values(1, 'INDIA', true)", trinoOldTableName), 1);
        assertUpdate(format("INSERT INTO %s values(2, 'USA', false)", trinoOldTableName), 1);
        assertThat(query(format("SELECT * FROM %s", trinoOldTableName)))
                .matches("VALUES ROW(INT '1', VARCHAR 'INDIA', BOOLEAN 'true'), ROW(INT '2', VARCHAR 'USA', BOOLEAN 'false')");

        String tableLocation = getTableLocation(trinoOldTableName);
        // Drop old table's hive metadata
        metastore.dropTable(TEST_SCHEMA_NAME, baseOldTableName, false);
        assertThat(metastore.getTable(TEST_SCHEMA_NAME, baseOldTableName)).as("Table metastore in Hive should be dropped").isEmpty();

        String baseNewTableName = "test_register_table_with_table_location_new_" + randomTableSuffix() + "_" + storageFormat;
        String trinoNewTableName = getTrinoTableName(baseNewTableName);

        assertUpdate(format("CALL iceberg.system.register_table ('%s', '%s', '%s')", TEST_SCHEMA_NAME, baseNewTableName, tableLocation));

        assertThat(query(format("SELECT * FROM %s", trinoNewTableName)))
                .matches("VALUES ROW(INT '1', VARCHAR 'INDIA', BOOLEAN 'true'), ROW(INT '2', VARCHAR 'USA', BOOLEAN 'false')");
        assertThat(getFormat(trinoNewTableName)).matches(storageFormat.name());
        assertUpdate(format("DROP TABLE %s", trinoNewTableName));
    }

    @Test(dataProvider = "storageFormats")
    public void testRegisterTableWithMetadataFile(StorageFormat storageFormat)
    {
        String baseOldTableName = "test_register_table_with_metadata_file_old_" + randomTableSuffix() + "_" + storageFormat;
        String trinoOldTableName = getTrinoTableName(baseOldTableName);

        assertUpdate(format("CREATE TABLE %s (a int, b varchar, c boolean) with (format = '%s')", trinoOldTableName, storageFormat));
        assertUpdate(format("INSERT INTO %s values(1, 'INDIA', true)", trinoOldTableName), 1);
        assertUpdate(format("INSERT INTO %s values(2, 'USA', false)", trinoOldTableName), 1);
        assertThat(query(format("SELECT * FROM %s", trinoOldTableName)))
                .matches("VALUES ROW(INT '1', VARCHAR 'INDIA', BOOLEAN 'true'), ROW(INT '2', VARCHAR 'USA', BOOLEAN 'false')");

        String tableLocation = getTableLocation(trinoOldTableName);
        // Drop old table's hive metadata
        metastore.dropTable(TEST_SCHEMA_NAME, baseOldTableName, false);
        assertThat(metastore.getTable(TEST_SCHEMA_NAME, baseOldTableName)).as("Table metastore in Hive should be dropped").isEmpty();

        String baseNewTableName = "test_register_table_with_metadata_file_new_" + randomTableSuffix() + "_" + storageFormat;
        String trinoNewTableName = getTrinoTableName(baseNewTableName);

        String metadataFileName = new File(getLatestMetadataLocation(trinoFileSystem, tableLocation).get()).getName();
        assertUpdate(format("CALL iceberg.system.register_table ('%s', '%s', '%s', '%s')", TEST_SCHEMA_NAME, baseNewTableName, tableLocation, metadataFileName));

        assertThat(query(format("SELECT * FROM %s", trinoNewTableName)))
                .matches("VALUES ROW(INT '1', VARCHAR 'INDIA', BOOLEAN 'true'), ROW(INT '2', VARCHAR 'USA', BOOLEAN 'false')");
        assertThat(getFormat(trinoNewTableName)).matches(storageFormat.name());
        assertUpdate(format("DROP TABLE %s", trinoNewTableName));
    }

    @Test(dataProvider = "storageFormats")
    public void testRegisterTableWithShowCreateTable(StorageFormat storageFormat)
    {
        String baseOldTableName = "test_register_table_with_show_create_table_old_" + randomTableSuffix() + "_" + storageFormat;
        String trinoOldTableName = getTrinoTableName(baseOldTableName);

        assertUpdate(format("CREATE TABLE %s (a int, b varchar, c boolean) with (format = '%s', partitioning = ARRAY['a'])", trinoOldTableName, storageFormat));
        assertUpdate(format("COMMENT ON TABLE %s IS '%s'", trinoOldTableName, "This is table comment"));
        assertUpdate(format("COMMENT ON COLUMN %s.%s IS '%s'", trinoOldTableName, "b", "This is column comment"));
        assertUpdate(format("INSERT INTO %s values(1, 'INDIA', true)", trinoOldTableName), 1);
        assertUpdate(format("INSERT INTO %s values(2, 'USA', false)", trinoOldTableName), 1);
        assertThat(query(format("SELECT * FROM %s", trinoOldTableName)))
                .matches("VALUES ROW(INT '1', VARCHAR 'INDIA', BOOLEAN 'true'), ROW(INT '2', VARCHAR 'USA', BOOLEAN 'false')");

        String showCreateTableOld = (String) computeActual("SHOW CREATE TABLE " + trinoOldTableName).getOnlyValue();

        String tableLocation = getTableLocation(trinoOldTableName);
        // Drop old table's hive metadata
        metastore.dropTable(TEST_SCHEMA_NAME, baseOldTableName, false);
        assertThat(metastore.getTable(TEST_SCHEMA_NAME, baseOldTableName)).as("Table metastore in Hive should be dropped").isEmpty();

        String baseNewTableName = "test_register_table_with_show_create_table_new_" + randomTableSuffix() + "_" + storageFormat;
        String trinoNewTableName = getTrinoTableName(baseNewTableName);

        assertUpdate(format("CALL iceberg.system.register_table ('%s', '%s', '%s')", TEST_SCHEMA_NAME, baseNewTableName, tableLocation));
        assertThat(query(format("SELECT * FROM %s", trinoNewTableName)))
                .matches("VALUES ROW(INT '1', VARCHAR 'INDIA', BOOLEAN 'true'), ROW(INT '2', VARCHAR 'USA', BOOLEAN 'false')");

        String showCreateTableNew = (String) computeActual("SHOW CREATE TABLE " + trinoNewTableName).getOnlyValue();

        assertThat(showCreateTableOld.replaceFirst(trinoOldTableName, trinoNewTableName)).isEqualTo(showCreateTableNew);
        assertUpdate(format("DROP TABLE %s", trinoNewTableName));
    }

    @Test
    public void testRegisterTableWithInvalidTableLocationAndMetadataFile()
    {
        StorageFormat storageFormat = StorageFormat.ORC;
        String baseOldTableName = "test_register_table_with_invalid_table_location_and_metadata_file_old_" + randomTableSuffix() + "_" + storageFormat;
        String trinoOldTableName = getTrinoTableName(baseOldTableName);

        assertUpdate(format("CREATE TABLE %s (a int, b varchar, c boolean) with (format = '%s')", trinoOldTableName, storageFormat));
        assertUpdate(format("INSERT INTO %s values(1, 'INDIA', true)", trinoOldTableName), 1);
        assertUpdate(format("INSERT INTO %s values(2, 'USA', false)", trinoOldTableName), 1);
        assertThat(query(format("SELECT * FROM %s", trinoOldTableName)))
                .matches("VALUES ROW(INT '1', VARCHAR 'INDIA', BOOLEAN 'true'), ROW(INT '2', VARCHAR 'USA', BOOLEAN 'false')");

        String tableLocation = getTableLocation(trinoOldTableName);
        String baseNewTableName = "test_register_table_with_invalid_table_location_and_metadata_file_new_" + randomTableSuffix() + "_" + storageFormat;
        String metadataFileName = "invalid_metadata_file.json";

        assertQueryFails(format("CALL iceberg.system.register_table ('%s', '%s', '%s', '%s')", TEST_SCHEMA_NAME, baseNewTableName, tableLocation, metadataFileName),
                "Location (.*) does not exist.*");

        // Drop table to verify register_table call fails
        assertUpdate(format("DROP TABLE %s", trinoOldTableName));

        assertQueryFails(format("CALL iceberg.system.register_table ('%s', '%s', '%s')", TEST_SCHEMA_NAME, baseNewTableName, tableLocation),
                "Location (.*) does not exist.*");
    }

    @Test
    public void testRegisterTableWithNoMetadataFile()
            throws IOException
    {
        StorageFormat storageFormat = StorageFormat.ORC;
        String baseOldTableName = "test_register_table_with_no_metadata_file_old_" + randomTableSuffix() + "_" + storageFormat;
        String trinoOldTableName = getTrinoTableName(baseOldTableName);

        assertUpdate(format("CREATE TABLE %s (a int, b varchar, c boolean) with (format = '%s')", trinoOldTableName, storageFormat));
        assertUpdate(format("INSERT INTO %s values(1, 'INDIA', true)", trinoOldTableName), 1);
        assertUpdate(format("INSERT INTO %s values(2, 'USA', false)", trinoOldTableName), 1);
        assertThat(query(format("SELECT * FROM %s", trinoOldTableName)))
                .matches("VALUES ROW(INT '1', VARCHAR 'INDIA', BOOLEAN 'true'), ROW(INT '2', VARCHAR 'USA', BOOLEAN 'false')");

        String tableLocation = getTableLocation(trinoOldTableName);
        String baseNewTableName = "test_register_table_with_no_metadata_file_new_" + randomTableSuffix() + "_" + storageFormat;

        // Delete files under metadata directory to verify register_table call fails
        deleteRecursively(Path.of(tableLocation, METADATA_FOLDER_NAME), ALLOW_INSECURE);

        assertQueryFails(format("CALL iceberg.system.register_table ('%s', '%s', '%s')", TEST_SCHEMA_NAME, baseNewTableName, tableLocation),
                "No metadata file exists at location.*");

        // TODO how to drop table whose metadata files are deleted?
        metastore.dropTable(TEST_SCHEMA_NAME, baseOldTableName, true);
    }

    @Test
    public void testRegisterTableWithInvalidParameters()
    {
        StorageFormat storageFormat = StorageFormat.ORC;
        String baseOldTableName = "test_register_table_with_invalid_parameter_old_" + randomTableSuffix() + "_" + storageFormat;
        String trinoOldTableName = getTrinoTableName(baseOldTableName);

        assertUpdate(format("CREATE TABLE %s (a int, b varchar, c boolean) with (format = '%s')", trinoOldTableName, storageFormat));
        assertUpdate(format("INSERT INTO %s values(1, 'INDIA', true)", trinoOldTableName), 1);
        assertUpdate(format("INSERT INTO %s values(2, 'USA', false)", trinoOldTableName), 1);
        assertThat(query(format("SELECT * FROM %s", trinoOldTableName)))
                .matches("VALUES ROW(INT '1', VARCHAR 'INDIA', BOOLEAN 'true'), ROW(INT '2', VARCHAR 'USA', BOOLEAN 'false')");

        String tableLocation = getTableLocation(trinoOldTableName);
        String baseNewTableName = "test_register_table_with_invalid_parameter_new_" + randomTableSuffix() + "_" + storageFormat;

        assertQueryFails(format("CALL iceberg.system.register_table ('%s', '%s')", TEST_SCHEMA_NAME, baseNewTableName),
                ".*'TABLE_LOCATION' is missing.*");
        assertQueryFails(format("CALL iceberg.system.register_table ('%s', null)", TEST_SCHEMA_NAME),
                ".*'TABLE_LOCATION' is missing.*");
        assertQueryFails("CALL iceberg.system.register_table (null, null)",
                ".*'TABLE_LOCATION' is missing.*");
        assertQueryFails(format("CALL iceberg.system.register_table ('%s')", TEST_SCHEMA_NAME),
                ".*'TABLE_NAME' is missing.*");
        assertQueryFails("CALL iceberg.system.register_table (null)",
                ".*'TABLE_NAME' is missing.*");
        assertQueryFails("CALL iceberg.system.register_table ()",
                ".*'SCHEMA_NAME' is missing.*");

        assertQueryFails(format("CALL iceberg.system.register_table ('%s', '%s', null)", TEST_SCHEMA_NAME, baseNewTableName),
                ".*Illegal parameter set passed((.|\\n)*)");
        assertQueryFails(format("CALL iceberg.system.register_table ('%s', null, null)", TEST_SCHEMA_NAME),
                ".*Illegal parameter set passed((.|\\n)*)");
        assertQueryFails("CALL iceberg.system.register_table (null, null, null)",
                ".*Illegal parameter set passed((.|\\n)*)");

        assertQueryFails(format("CALL iceberg.system.register_table ('%s', '%s', '%s')", "invalid_schema", baseNewTableName, tableLocation),
                ".*Schema '(.*)' does not exist.*");
        assertQueryFails(format("CALL iceberg.system.register_table ('%s', '%s', '%s')", TEST_SCHEMA_NAME, baseOldTableName, tableLocation),
                ".*Table '(.*)' already exists in schema '(.*)'.*");

        assertUpdate(format("DROP TABLE %s", trinoOldTableName));
    }

    private static String getTrinoTableName(String tableName)
    {
        return format("%s.%s.%s", CATALOG, TEST_SCHEMA_NAME, tableName);
    }

    private String getTableLocation(String tableName)
    {
        String regex = ".*location = '(.*?)'.*";
        String text = (String) computeActual("SHOW CREATE TABLE " + tableName).getOnlyValue();
        return getRegexMatch(text, regex);
    }

    private String getFormat(String tableName)
    {
        String regex = ".*format = '(.*?)'.*";
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
