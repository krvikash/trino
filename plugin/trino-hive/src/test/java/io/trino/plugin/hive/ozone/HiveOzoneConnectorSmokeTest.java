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
package io.trino.plugin.hive.ozone;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Level;
import io.airlift.log.Logging;
import io.trino.Session;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.metastore.Database;
import io.trino.metastore.HiveMetastore;
import io.trino.plugin.hive.HiveQueryRunner;
import io.trino.plugin.hive.TestHiveConnectorSmokeTest;
import io.trino.plugin.hive.containers.HiveHadoop;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.security.Identity;
import io.trino.spi.security.PrincipalType;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import io.trino.tpch.TpchTable;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Optional;

import static io.trino.hdfs.HdfsTestUtils.HDFS_FILE_SYSTEM_FACTORY;
import static io.trino.plugin.hive.metastore.file.TestingFileHiveMetastore.createTestingFileHiveMetastore;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.QueryAssertions.copyTpchTables;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

final class HiveOzoneConnectorSmokeTest
        extends TestHiveConnectorSmokeTest
{
    private static final String CATALOG_NAME = "hive";
    private static final String SCHEMA_NAME = "ozone_" + randomNameSuffix();

    private final String volumeName = "volume-" + randomNameSuffix();
    private final String bucketName = "bucket-" + randomNameSuffix();

    @AutoClose
    private final HiveOzoneDataLake hiveOzoneDataLake = new HiveOzoneDataLake();

    private final String pathToBucket = "%s/%s/%s".formatted(hiveOzoneDataLake.getApacheOzoneContainer().getOfsEndpointAddress(), volumeName, bucketName);

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Logging.initialize().setLevel("org.apache.hadoop.fs", Level.DEBUG);

        HiveHadoop hiveHadoop = hiveOzoneDataLake.getHiveHadoop();
        hiveHadoop.executeInContainer("hdfs", "dfs", "-mkdir", "-p", "/%s/%s".formatted(volumeName, bucketName));
        // File metastore is used, because, to create a database, the very same location has to be routable from metastore and SEP
        HiveMetastore metastore = createTestingFileHiveMetastore(HDFS_FILE_SYSTEM_FACTORY, Location.of(pathToBucket));
        metastore.createDatabase(Database.builder()
                .setDatabaseName(SCHEMA_NAME)
                .setOwnerName(Optional.of("public"))
                .setOwnerType(Optional.of(PrincipalType.ROLE))
                .build());
        DistributedQueryRunner queryRunner = HiveQueryRunner.builder()
                .setMetastore(_ -> metastore)
                .amendSession(sessionBuilder -> sessionBuilder.setCatalog(CATALOG_NAME).setSchema(SCHEMA_NAME))
                .setHiveProperties(ImmutableMap.<String, String>builder()
                        .put("hive.non-managed-table-writes-enabled", "true")
                        .put("hive.security", "allow-all")
                        // Do not use temporary directory as rename is not supported
                        // Caused by: java.io.IOException: Directory rename from ofs://ozone-manager:9862/tmp/presto-hive/200f30c5-5083-4bba-b670-e2b876272d9c to ofs://ozone-manager:9862/volume1/bucket1/ozone/nation failed: Cannot rename a key to a different bucket
                        // Caused by: java.io.IOException: Cannot rename a key to a different bucket
                        .put("hive.temporary-staging-directory-enabled", "false")
                        .buildOrThrow())
                .build();
        queryRunner.execute(createSchemaSql(SCHEMA_NAME));
        copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, queryRunner.getDefaultSession(), ImmutableList.<TpchTable<?>>builder()
                .addAll(REQUIRED_TPCH_TABLES)
                .build());
        return queryRunner;
    }

    @Override
    protected String createSchemaSql(String schemaName)
    {
        return "CREATE SCHEMA IF NOT EXISTS %s WITH (location = '%s/%s')"
                .formatted(schemaName, pathToBucket, schemaName);
    }

    @Test
    public void testSchemaLocation()
    {
        assertThat((String) computeScalar("SHOW CREATE SCHEMA " + SCHEMA_NAME))
                .isEqualTo(
                        """
                        CREATE SCHEMA %s.%s
                        WITH (
                           location = '%s/%s'
                        )""".formatted(
                                CATALOG_NAME,
                                SCHEMA_NAME,
                                pathToBucket,
                                SCHEMA_NAME));
    }

    @Test
    public void testTableLocation()
    {
        try (TestTable table = newTrinoTable("test_table_location", getCreateTableDefaultDefinition())) {
            assertUpdate("INSERT INTO %s (a, b) VALUES (42, -38.5), (13, 99.9)".formatted(table.getName()), 2);
            List<MaterializedRow> paths = getQueryRunner().execute(getSession(), "SELECT distinct \"$path\" FROM " + table.getName()).getMaterializedRows();
            paths.stream().map(row -> row.getField(0))
                    .forEach(path -> assertThat(path).asString().startsWith(pathToBucket));
        }
    }

    @Test
    @Override
    public void testShowCreateTable()
    {
        assertThat((String) computeScalar("SHOW CREATE TABLE region"))
                .matches(
                        """
                        CREATE TABLE \\w+\\.\\w+\\.region \\Q(
                           regionkey bigint,
                           name varchar(25),
                           comment varchar(152)
                        )
                        WITH (
                           format = 'ORC'
                        )""");
    }

    @Test
    @Override
    public void testCreateSchemaWithNonLowercaseOwnerName()
    {
        Session newSession = Session.builder(getSession())
                .setIdentity(Identity.ofUser("ADMIN"))
                .build();
        String schemaName = "test_schema_create_uppercase_owner_name_" + randomNameSuffix();
        assertUpdate(newSession, createSchemaSql(schemaName));
        assertThat(query(newSession, "SHOW SCHEMAS"))
                .skippingTypesCheck()
                .containsAll(format("VALUES '%s'", schemaName));
        assertUpdate(newSession, "DROP SCHEMA " + schemaName);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testSelectTableWithZeroByteSuccessMarkerFile(boolean partitioned)
            throws Exception
    {
        String tableDefinition = "(a double, b bigint)" + (partitioned ? " WITH (partitioned_by = ARRAY['b'])" : "");
        try (TestTable table = newTrinoTable("test_success_marker", tableDefinition)) {
            assertUpdate("INSERT INTO %s VALUES (-38.5, 42)".formatted(table.getName()), 1);

            String dataDirectory = getDataDirectory(table.getName());
            assertThat(dataDirectory).endsWith(partitioned ? "/b=42" : table.getName());

            // Simulate a Spark/MR job writing a zero byte _SUCCESS marker into the data directory
            TrinoFileSystem fileSystem = HDFS_FILE_SYSTEM_FACTORY.create(ConnectorIdentity.ofUser("test"));
            Location successMarker = Location.of(dataDirectory + "/_SUCCESS");
            fileSystem.newOutputFile(successMarker).create().close();

            assertThat(fileSystem.newInputFile(successMarker).exists()).isTrue();
            assertThat(fileSystem.newInputFile(successMarker).length()).isEqualTo(0L);

            assertThat(query("SELECT * FROM " + table.getName()))
                    .result()
                    .matches("VALUES (DOUBLE '-38.5', BIGINT '42')");
        }
    }

    private String getDataDirectory(String tableName)
    {
        List<MaterializedRow> paths = computeActual("SELECT distinct \"$path\" FROM " + tableName).getMaterializedRows();
        paths.stream().map(row -> row.getField(0))
                .forEach(path -> assertThat(path).asString()
                        .startsWith("ofs://")
                        .startsWith(pathToBucket));
        assertThat(paths.size()).isEqualTo(1);
        String dataFilePath = (String) paths.getFirst().getField(0);
        return dataFilePath.substring(0, dataFilePath.lastIndexOf('/'));
    }
}
