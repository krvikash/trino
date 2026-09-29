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
package io.trino.plugin.sas;

import io.trino.filesystem.Location;
import io.trino.filesystem.s3.S3FileSystemFactory;
import io.trino.spi.security.ConnectorIdentity;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;

import static io.trino.plugin.sas.SasConfig.MappingType.JSON;
import static io.trino.plugin.sas.SasFlociStorage.location;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

/**
 * Exercises {@link SasNasClient} and {@link SasNasJsonClient} against S3 (Floci): prefix-based
 * schema listing, recursive object listing, case-insensitive lookups, and zero-byte directory
 * marker objects, none of which have a local filesystem equivalent.
 */
@TestInstance(PER_CLASS)
final class TestSasS3NasClient
{
    private static final ConnectorIdentity TEST_IDENTITY = ConnectorIdentity.ofUser("test");
    private static final String BUCKET = "sas-client-test";
    private static final String PREFIX = "sas/root";

    private SasFlociStorage storage;
    private S3FileSystemFactory fileSystemFactory;
    private SasNasClient fsClient;
    private SasNasJsonClient jsonClient;

    @BeforeAll
    void setUp()
            throws IOException
    {
        storage = SasFlociStorage.start();
        storage.createBucket(BUCKET);
        Path resources = Path.of("src/test/resources").toAbsolutePath();
        storage.uploadDirectory(resources, BUCKET, PREFIX);
        // zero-byte directory markers, as created by the AWS console "Create folder" action
        storage.putObject(BUCKET, PREFIX + "/schema1/", new byte[0]);
        storage.putObject(BUCKET, PREFIX + "/markers_only/", new byte[0]);

        fileSystemFactory = storage.createFileSystemFactory();
        Location rootLocation = Location.of(location(BUCKET, PREFIX));
        fsClient = new SasNasClient(rootLocation, fileSystemFactory);

        SasConfig jsonConfig = new SasConfig()
                .setDataDirectory(URI.create(rootLocation.toString()))
                .setMappingType(JSON)
                .setJsonFile(resources.resolve("mapping_case_test.json").toFile());
        jsonClient = new SasNasJsonClient(rootLocation, jsonConfig, fileSystemFactory);
    }

    @AfterAll
    void tearDown()
    {
        // setUp may have failed part-way, so close whatever was started
        try {
            if (fileSystemFactory != null) {
                fileSystemFactory.destroy();
            }
        }
        finally {
            fileSystemFactory = null;
            if (storage != null) {
                storage.close();
                storage = null;
            }
        }
    }

    @Test
    void testSchemaNamesAreLowercasePrefixes()
    {
        assertThat(fsClient.getSchemaNames(TEST_IDENTITY))
                .containsExactlyInAnyOrder("schema1", "schema2", "schema3", "prod", "data", "markers_only");
    }

    @Test
    void testDirectoryMarkerObjectsAreIgnored()
    {
        assertThat(fsClient.getTableNames("schema1", TEST_IDENTITY)).containsExactly("colon");
        assertThat(fsClient.getTableNames("markers_only", TEST_IDENTITY)).isEmpty();
        assertThat(fsClient.getTable("markers_only", "anything", TEST_IDENTITY)).isEmpty();
    }

    @Test
    void testNestedTableNames()
    {
        assertThat(fsClient.getTableNames("data", TEST_IDENTITY)).containsExactly("year2024/reports/sales_report");
        SasTable table = fsClient.getTable("data", "year2024/reports/sales_report", TEST_IDENTITY).orElseThrow();
        assertThat(table.source()).isEqualTo(location(BUCKET, PREFIX) + "/data/year2024/reports/sales_report.sas7bdat");
    }

    @Test
    void testGetTableIsCaseInsensitive()
    {
        assertThat(fsClient.getTable("SCHEMA1", "COLON", TEST_IDENTITY).orElseThrow().lineCount()).isEqualTo(15564L);
        SasTable upper = fsClient.getTable("schema3", "upper", TEST_IDENTITY).orElseThrow();
        assertThat(upper.lineCount()).isEqualTo(15564L);
        assertThat(upper.source()).isEqualTo(location(BUCKET, PREFIX) + "/SCHEMA3/UPPER.sas7bdat");
    }

    @Test
    void testGetTableReadsHeaderAndColumns()
    {
        SasTable table = fsClient.getTable("prod", "param_gen_nn1", TEST_IDENTITY).orElseThrow();
        assertThat(table.lineCount()).isEqualTo(328L);
        assertThat(table.pageCount()).isEqualTo(3L);
        assertThat(table.columns()).isNotEmpty();
        assertThat(fsClient.listColumnsFromFile(table.source(), fileSystemFactory.create(TEST_IDENTITY)))
                .isEqualTo(table.columns());
    }

    @Test
    void testMissingSchemaAndTable()
    {
        assertThat(fsClient.getTableNames("missing", TEST_IDENTITY)).isEmpty();
        assertThat(fsClient.getTable("missing", "colon", TEST_IDENTITY)).isEmpty();
        assertThat(fsClient.getTable("schema1", "missing", TEST_IDENTITY)).isEmpty();
    }

    @Test
    void testJsonExplicitTable()
    {
        assertThat(jsonClient.getSchemaNames(TEST_IDENTITY)).contains("explicit_schema", "path_schema", "upper_path_schema");
        assertThat(jsonClient.getTableNames("explicit_schema", TEST_IDENTITY)).containsExactly("explicit_table");
        SasTable table = jsonClient.getTable("EXPLICIT_SCHEMA", "EXPLICIT_TABLE", TEST_IDENTITY).orElseThrow();
        assertThat(table.lineCount()).isEqualTo(15564L);
        assertThat(table.source()).isEqualTo(location(BUCKET, PREFIX) + "/schema1/colon.sas7bdat");
    }

    @Test
    void testJsonPathSchema()
    {
        assertThat(jsonClient.getTableNames("path_schema", TEST_IDENTITY)).containsExactly("colon");
        assertThat(jsonClient.getTable("path_schema", "COLON", TEST_IDENTITY).orElseThrow().lineCount()).isEqualTo(15564L);
    }

    @Test
    void testJsonPathSchemaWithUppercaseObjects()
    {
        assertThat(jsonClient.getTableNames("upper_path_schema", TEST_IDENTITY)).containsExactly("upper");
        assertThat(jsonClient.getTable("upper_path_schema", "upper", TEST_IDENTITY).orElseThrow().lineCount()).isEqualTo(15564L);
    }
}
