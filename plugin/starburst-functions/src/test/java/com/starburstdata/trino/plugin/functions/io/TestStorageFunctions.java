/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.functions.io;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.starburstdata.trino.plugin.functions.TestingFunctionsPlugin;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.s3.S3FileSystemConfig;
import io.trino.filesystem.s3.S3FileSystemFactory;
import io.trino.filesystem.s3.S3FileSystemStats;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.HiveMetastoreFactory;
import io.trino.plugin.hive.HiveConnector;
import io.trino.plugin.hive.HivePlugin;
import io.trino.plugin.hive.HiveStorageFormat;
import io.trino.plugin.hive.LocationAccessControl;
import io.trino.plugin.hive.containers.Hive3MinioDataLake;
import io.trino.plugin.hive.containers.HiveMinioDataLake;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.security.AccessDeniedException;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.containers.Minio;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.Minio.MINIO_REGION;
import static io.trino.testing.containers.Minio.MINIO_ROOT_PASSWORD;
import static io.trino.testing.containers.Minio.MINIO_ROOT_USER;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.EnumSource.Mode.EXCLUDE;

final class TestStorageFunctions
        extends AbstractTestQueryFramework
{
    private TrinoFileSystem fileSystem;
    private HiveMetastore metastore;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        HiveMinioDataLake container = closeAfterClass(new Hive3MinioDataLake("test-bucket"));
        container.start();
        Minio minio = container.getMinio();

        Path credentialsFile = createCredentialsFile(minio.getMinioAddress());
        closeAfterClass(() -> Files.delete(credentialsFile));

        DistributedQueryRunner queryRunner = StorageQueryRunner.builder()
                .addCoordinatorProperty("sql.path", "starburst.io")
                .addConnectorProperty("io.credentials-file", credentialsFile.toAbsolutePath().toString())
                .setPlugin(new TestingFunctionsPlugin(new DenyLocationAccessControlModule()))
                .amendSession(sessionBuilder -> sessionBuilder.setCatalog("hive").setSchema("tpch"))
                .build();

        queryRunner.installPlugin(new HivePlugin());
        queryRunner.createCatalog("hive", "hive", ImmutableMap.<String, String>builder()
                .put("hive.metastore.uri", container.getHiveMetastoreEndpoint().toString())
                .put("fs.s3.enabled", "true")
                .put("s3.aws-access-key", MINIO_ROOT_USER)
                .put("s3.aws-secret-key", MINIO_ROOT_PASSWORD)
                .put("s3.region", MINIO_REGION)
                .put("s3.endpoint", "http://" + container.getMinio().getMinioApiEndpoint())
                .put("s3.path-style-access", "true")
                .buildOrThrow());
        queryRunner.execute("CREATE SCHEMA hive.tpch WITH (location = 's3://test-bucket/tpch')");
        metastore = ((HiveConnector) queryRunner.getCoordinator().getConnector("hive")).getInjector()
                .getInstance(HiveMetastoreFactory.class)
                .createMetastore(Optional.empty());

        ConnectorSession session = queryRunner.getDefaultSession().toConnectorSession();
        fileSystem = fileSystem(session, minio);

        return queryRunner;
    }

    @Test
    void testLoadWithSchemaDiscovery()
    {
        try (TestTable table = newTrinoTable("test", "WITH (format = 'PARQUET') AS SELECT * FROM tpch.tiny.region")) {
            String tableLocation = loadTableLocation(table.getName());
            assertThat(query("SELECT * FROM TABLE(load('" + tableLocation + "/'))"))
                    .skippingTypesCheck()
                    .matches("SELECT * FROM " + table.getName());

            String filePath = (String) computeScalar("SELECT DISTINCT \"$path\" FROM " + table.getName());
            assertQueryFails("SELECT * FROM TABLE(load('" + filePath + "'))", ".*Root directory is empty or isn't a directory.*");
        }
    }

    @Test
    void testLoadPartitionTable()
    {
        try (TestTable table = newTrinoTable(
                "test_partitioned",
                "(id int, part varchar) WITH (partitioned_by = ARRAY['part'])",
                List.of("1, 'part1'", "2, 'part1'", "3, 'part2'", "4, 'part2'"))) {
            String tableLocation = loadTableLocation(table.getName());

            assertThat(query("SELECT * FROM TABLE(load('" + tableLocation + "/'))"))
                    .matches("VALUES 1, 2, 3, 4");
            assertThat(query("SELECT * FROM TABLE(load('" + tableLocation + "/part=part1/'))"))
                    .matches("VALUES 1, 2");
            assertThat(query("SELECT * FROM TABLE(load('" + tableLocation + "/part=part2/'))"))
                    .matches("VALUES 3, 4");

            assertThat(query("SELECT * FROM TABLE(load('" + tableLocation + "/', 'ORC', DESCRIPTOR(\"id\" INT)))"))
                    .matches("VALUES 1, 2, 3, 4");
            assertThat(query("SELECT * FROM TABLE(load('" + tableLocation + "/part=part1/', 'ORC', DESCRIPTOR(\"id\" INT)))"))
                    .matches("VALUES 1, 2");
            assertThat(query("SELECT * FROM TABLE(load('" + tableLocation + "/part=part2/', 'ORC', DESCRIPTOR(\"id\" INT)))"))
                    .matches("VALUES 3, 4");
        }
    }

    @Test
    void testIgnoreHiddenFile()
            throws Exception
    {
        Location location = Location.of("s3://test-bucket/hidden");
        fileSystem.createDirectory(location);
        fileSystem.newOutputFile(location.appendPath("data.csv")).createExclusive("test csv".getBytes(UTF_8));
        fileSystem.newOutputFile(location.appendPath(".hidden.csv")).createExclusive("hidden dot csv".getBytes(UTF_8));
        fileSystem.newOutputFile(location.appendPath("_hidden.csv")).createExclusive("hidden underscore csv".getBytes(UTF_8));

        assertThat(query("SELECT * FROM TABLE(load('s3://test-bucket/hidden/'))"))
                .matches("VALUES VARCHAR 'test csv'");
    }

    /**
     * Schema discovery supports only Parquet, ORC, CSV, and JSON formats.
     * CSV is tested in {@link #testCsvWithSchemaDiscovery}.
     * JSON is tested in {@link #testJsonWithSchemaDiscovery}.
     */
    @ParameterizedTest
    @EnumSource(names = {"PARQUET", "ORC"})
    void testLoadFormatWithSchemaDiscovery(HiveStorageFormat format)
    {
        try (TestTable table = newTrinoTable("test_format", "WITH (format = '" + format + "') AS SELECT * FROM tpch.tiny.region")) {
            String tableLocation = loadTableLocation(table.getName());
            assertThat(query("SELECT * FROM TABLE(load('" + tableLocation + "/'))"))
                    .skippingTypesCheck()
                    .matches("SELECT * FROM " + table.getName());
        }
    }

    @Test
    void testCsvWithSchemaDiscovery()
            throws Exception
    {
        Location location = Location.of("s3://test-bucket/csv");
        fileSystem.createDirectory(location);
        fileSystem.newOutputFile(location.appendPath("test.csv")).createExclusive("test csv".getBytes(UTF_8));

        assertThat(query("SELECT * FROM TABLE(load('s3://test-bucket/csv/'))"))
                .matches("VALUES VARCHAR 'test csv'");
    }

    @Test
    void testMultiLineCsv()
            throws Exception
    {
        Location location = Location.of("s3://%s/multiline-csv/multiline.csv".formatted("test-bucket"));
        byte[] bytes = Resources.toByteArray(Resources.getResource("multiline.csv"));
        fileSystem.newOutputFile(location).createExclusive(bytes);

        assertThat(query("SELECT * FROM TABLE(load(" +
                "location => 's3://test-bucket/multiline-csv/'," +
                "format => 'CSV'," +
                "columns => DESCRIPTOR(id VARCHAR, name VARCHAR, comments VARCHAR, city VARCHAR)," +
                "skip_header => 1, " +
                "multiline => true))")).skippingTypesCheck()
                .matches("VALUES" +
                        "(NULL, NULL, NULL, NULL)," +
                        "('1', 'Name1', 'This is line 1\r\nThis is line 2\r\nThis is line 3', 'Delhi')," +
                        "('2', 'Name2', 'First line\r\nSecond line', '')," +
                        "('3', '', '', 'Mumbai')," +
                        "('4', 'Name4', 'Address verification pending\r\nNeed approval from manager\r\nWaiting for response', 'Bangalore')," +
                        "('5', 'Name5', 'Row contains special characters: @#$%^&\r\nAnother line with numbers 12345', 'Chennai')," +
                        "('6', 'Name6', '', '')," +
                        "('7', 'Name7', 'Single line comment', 'Hyderabad')," +
                        "('8', 'Name8', 'Multiline test data\r\nFor spark csv read\r\nWith quoted values', 'Pune')");
    }

    @Test
    void testMultiLineCsvWithSameQuoteAndEscape()
            throws Exception
    {
        Location location = Location.of("s3://%s/multiline-same-quote-escape-csv/test.csv".formatted("test-bucket"));
        String data =
                """
                id,name,comments,city
                1,Name1,"This is line ""1
                This is line ""2
                This is line ""3",Delhi
                2,Name2,"First line
                Second line",""";
        byte[] bytes = data.getBytes(UTF_8);
        fileSystem.newOutputFile(location).createExclusive(bytes);

        assertThat(query("SELECT * FROM TABLE(load(" +
                "location => 's3://test-bucket/multiline-same-quote-escape-csv/'," +
                "format => 'CSV'," +
                "columns => DESCRIPTOR(id VARCHAR, name VARCHAR, comments VARCHAR, city VARCHAR)," +
                "skip_header => 1, " +
                "quote_char => '\"', " +
                "escape_char => '\"', " +
                "multiline => true))")).skippingTypesCheck()
                .matches("VALUES" +
                        "('1', 'Name1', 'This is line \"1\nThis is line \"2\nThis is line \"3', 'Delhi')," +
                        "('2', 'Name2', 'First line\nSecond line', '')");
    }

    @Test
    void testJsonWithSchemaDiscovery()
            throws Exception
    {
        Location location = Location.of("s3://test-bucket/json");
        fileSystem.createDirectory(location);
        fileSystem.newOutputFile(location.appendPath("test.json")).createExclusive("{ \"id\": 1, \"name\": \"alice\" }".getBytes(UTF_8));

        assertThat(query("SELECT * FROM TABLE(load('s3://test-bucket/json/'))"))
                .matches("VALUES (1, VARCHAR 'alice')");
    }

    @Test
    void testLoadWithDescriptor()
    {
        try (TestTable table = newTrinoTable("test_descriptor", "WITH (format = 'PARQUET') AS SELECT * FROM tpch.tiny.region")) {
            String tableLocation = loadTableLocation(table.getName());
            assertThat(query("SELECT * FROM TABLE(load('" + tableLocation + "/', 'PARQUET', DESCRIPTOR(\"regionkey\" BIGINT, \"name\" VARCHAR(25), \"comment\" VARCHAR(152))))"))
                    .matches("SELECT * FROM tpch.tiny.region");

            String filePath = (String) computeScalar("SELECT DISTINCT \"$path\" FROM " + table.getName());
            assertThat(query("SELECT * FROM TABLE(load('" + filePath + "', 'PARQUET', DESCRIPTOR(\"regionkey\" BIGINT, \"name\" VARCHAR(25), \"comment\" VARCHAR(152))))"))
                    .matches("SELECT * FROM tpch.tiny.region");
        }
    }

    @ParameterizedTest
    @EnumSource(mode = EXCLUDE, names = {"CSV", "ESRI", "ESRI_GEO_JSON", "REGEX", "SEQUENCEFILE_PROTOBUF"})
    void testLoadWithDescriptor(HiveStorageFormat format)
    {
        String location = "s3://%s/%s".formatted("test-bucket", randomNameSuffix());
        computeActual("SELECT * FROM TABLE(hive.system.unload(" +
                "input => TABLE(tpch.tiny.region)," +
                "location => '" + location + "/'," +
                "format => '" + format + "'))");

        assertThat(query("SELECT * FROM TABLE(load('" + location + "/', '" + format + "', DESCRIPTOR(\"regionkey\" BIGINT, \"name\" VARCHAR(25), \"comment\" VARCHAR(152))))"))
                .matches("SELECT * FROM tpch.tiny.region");
    }

    @Test
    void testLoadCsvWithDescriptor()
    {
        String location = "s3://%s/%s".formatted("test-bucket", randomNameSuffix());
        computeActual("SELECT * FROM TABLE(hive.system.unload(" +
                "input => TABLE(SELECT name, comment FROM tpch.tiny.region)," +
                "location => '" + location + "'," +
                "format => 'CSV'))");

        assertThat(query("SELECT * FROM TABLE(load('" + location + "/', 'CSV', DESCRIPTOR(\"name\" VARCHAR, \"comment\" VARCHAR)))"))
                .matches("SELECT CAST(name AS VARCHAR), CAST(comment AS VARCHAR) FROM tpch.tiny.region");
    }

    @Test
    void testLoadCsvWithSkipHeader()
            throws Exception
    {
        Location location = Location.of("s3://test-bucket/csv_with_header");
        fileSystem.createDirectory(location);
        fileSystem.newOutputFile(location.appendPath("test.csv")).createExclusive("\"id\",\"name\"\n\"1\",\"alice\"\n\"2\",\"bob\"".getBytes(UTF_8));

        // Load without schema discovery
        assertThat(query("SELECT * FROM TABLE(load('s3://test-bucket/csv_with_header/', 'CSV', DESCRIPTOR(\"id\" VARCHAR, \"name\" VARCHAR), 1))"))
                .matches("VALUES (VARCHAR '1', VARCHAR 'alice'), ('2', 'bob')");
        assertThat(query("SELECT * FROM TABLE(load('s3://test-bucket/csv_with_header/', 'CSV', DESCRIPTOR(\"id\" VARCHAR, \"name\" VARCHAR), 0))"))
                .matches("VALUES (VARCHAR 'id', VARCHAR 'name'), ('1', 'alice'), ('2', 'bob')");

        // Load with schema discovery
        assertThat(query("SELECT * FROM TABLE(load(location=>'s3://test-bucket/csv_with_header/', skip_header=>1))"))
                .matches("VALUES (VARCHAR '1', VARCHAR 'alice'), ('2', 'bob')");
        assertThat(query("SELECT * FROM TABLE(load(location=>'s3://test-bucket/csv_with_header/', skip_header=>0))"))
                .matches("VALUES (VARCHAR 'id', VARCHAR 'name'), ('1', 'alice'), ('2', 'bob')");
    }

    @Test
    void testLoadTextfileWithSkipHeader()
            throws Exception
    {
        Location location = Location.of("s3://test-bucket/textfile_with_header");
        fileSystem.createDirectory(location);
        fileSystem.newOutputFile(location.appendPath("test.txt")).createExclusive("id\u0001name\n1\u0001alice\n2\u0001bob".getBytes(UTF_8));

        // Load without schema discovery
        assertThat(query("SELECT * FROM TABLE(load('s3://test-bucket/textfile_with_header/', 'TEXTFILE', DESCRIPTOR(\"id\" VARCHAR, \"name\" VARCHAR), 1))"))
                .matches("VALUES (VARCHAR '1', VARCHAR 'alice'), ('2', 'bob')");
        assertThat(query("SELECT * FROM TABLE(load('s3://test-bucket/textfile_with_header/', 'TEXTFILE', DESCRIPTOR(\"id\" VARCHAR, \"name\" VARCHAR), 0))"))
                .matches("VALUES (VARCHAR 'id', VARCHAR 'name'), ('1', 'alice'), ('2', 'bob')");

        // Load with schema discovery
        assertThat(query("SELECT * FROM TABLE(load(location=>'s3://test-bucket/textfile_with_header/', skip_header=>1))"))
                .matches("VALUES (VARCHAR '1', VARCHAR 'alice'), ('2', 'bob')");
        assertThat(query("SELECT * FROM TABLE(load(location=>'s3://test-bucket/textfile_with_header/', skip_header=>0))"))
                .matches("VALUES (VARCHAR 'id', VARCHAR 'name'), ('1', 'alice'), ('2', 'bob')");
    }

    @Test
    void testInvalidSkipHeaderForFormat()
    {
        assertQueryFails(
                "SELECT * FROM TABLE(load('s3://test-bucket/parquet/', 'PARQUET', DESCRIPTOR(\"id\" INT), 1))",
                "Cannot specify header for storage format: PARQUET");
        assertQueryFails(
                "SELECT * FROM TABLE(load('s3://test-bucket/orc/', 'ORC', DESCRIPTOR(\"id\" INT), 0))",
                "Cannot specify header for storage format: ORC");
        assertQueryFails(
                "SELECT * FROM TABLE(load('s3://test-bucket/orc/', 'CSV', DESCRIPTOR(\"id\" INT), -1))",
                "SKIP_HEADER must be >= 0");
    }

    @Test
    void testLoadAvroCaseSensitivity()
            throws Exception
    {
        Location location = Location.of("s3://%s/%s".formatted("test-bucket", randomNameSuffix()));
        byte[] bytes = Resources.toByteArray(Resources.getResource("case_sensitivity.avro"));
        fileSystem.newOutputFile(location).createExclusive(bytes);

        assertThat(query("SELECT * FROM TABLE(load('" + location + "', 'AVRO', DESCRIPTOR(id INTEGER, \"Name\" VARCHAR)))"))
                .matches("VALUES (1, VARCHAR 'alice'), (2, VARCHAR 'bob')");
    }

    @Test
    void testLocationAccessControl()
    {
        assertQueryFails(
                "SELECT * FROM TABLE(load(location=>'s3://test-bucket/denied/'))",
                "Access Denied: .*");
    }

    @Test
    void testInvalidArgument()
    {
        assertQueryFails(
                "SELECT * FROM TABLE(load(location=>NULL))",
                "location cannot be null");

        assertQueryFails(
                "SELECT * FROM TABLE(load(location=>'s3://dummy', format=>'PARQUET'))",
                "FORMAT and COLUMNS arguments must be both specified or both omitted");
        assertQueryFails(
                "SELECT * FROM TABLE(load(location=>'s3://dummy', columns=>DESCRIPTOR(\"regionkey\" BIGINT)))",
                "FORMAT and COLUMNS arguments must be both specified or both omitted");

        assertQueryFails(
                "SELECT * FROM TABLE(load(location=>'s3://dummy/test', format=>'PARQUET', columns=>DESCRIPTOR(\"id\" INT), field_separator=>','))",
                "Cannot specify field separator for storage format: PARQUET");
        assertQueryFails(
                "SELECT * FROM TABLE(load(location=>'s3://dummy', field_separator=>''))",
                "FIELD_SEPARATOR must be a single character string, but was: ''");
        assertQueryFails(
                "SELECT * FROM TABLE(load(location=>'s3://dummy', field_separator=>',,'))",
                "FIELD_SEPARATOR must be a single character string, but was: ',,'");

        assertQueryFails(
                "SELECT * FROM TABLE(load(location=>'s3://dummy/test', format=>'PARQUET', columns=>DESCRIPTOR(\"id\" INT), line_separator=>','))",
                "Cannot specify line separator for storage format: PARQUET");
        assertQueryFails(
                "SELECT * FROM TABLE(load(location=>'s3://dummy', line_separator=>''))",
                "LINE_SEPARATOR must be a single character string, but was: ''");
        assertQueryFails(
                "SELECT * FROM TABLE(load(location=>'s3://dummy', line_separator=>',,'))",
                "LINE_SEPARATOR must be a single character string, but was: ',,'");

        assertQueryFails(
                "SELECT * FROM TABLE(load(location=>'s3://dummy/test', format=>'PARQUET', columns=>DESCRIPTOR(\"id\" INT), quote_char=>','))",
                "Cannot specify quote for storage format: PARQUET");
        assertQueryFails(
                "SELECT * FROM TABLE(load(location=>'s3://dummy', quote_char=>''))",
                "QUOTE_CHAR must be a single character string, but was: ''");
        assertQueryFails(
                "SELECT * FROM TABLE(load(location=>'s3://dummy', quote_char=>',,'))",
                "QUOTE_CHAR must be a single character string, but was: ',,'");

        assertQueryFails(
                "SELECT * FROM TABLE(load(location=>'s3://dummy/test', format=>'PARQUET', columns=>DESCRIPTOR(\"id\" INT), escape_char=>','))",
                "Cannot specify escape for storage format: PARQUET");
        assertQueryFails(
                "SELECT * FROM TABLE(load(location=>'s3://dummy', escape_char=>''))",
                "ESCAPE_CHAR must be a single character string, but was: ''");
        assertQueryFails(
                "SELECT * FROM TABLE(load(location=>'s3://dummy', escape_char=>',,'))",
                "ESCAPE_CHAR must be a single character string, but was: ',,'");

        assertQueryFails(
                "SELECT * FROM TABLE(load(location=>'s3://dummy/test', format=>'PARQUET', columns=>DESCRIPTOR(\"id\" INT), multiline=>true))",
                "Cannot specify multiline for storage format: PARQUET");
        assertQueryFails(
                "SELECT * FROM TABLE(load(location=>'s3://dummy/test', format=>'CSV', columns=>DESCRIPTOR(\"id\" INT), multiline=>true, line_separator=>','))",
                "Cannot specify both multiline and line separator");
    }

    @Test
    void testInvalidLocation()
    {
        assertQueryFails(
                "SELECT * FROM TABLE(load(location=>'s3://test-bucket/slashes//'))",
                "Multiple trailing slashes are not supported.*");
    }

    @Test
    void testInvalidConfiguration()
    {
        assertQueryFails("SELECT * FROM TABLE(load(location=>'s3://test-duplicate/default_prefix/'))", ".* Invalid configuration: expected one element.*");
    }

    @Test
    void testLoadWithLineSeparator()
            throws Exception
    {
        Location location = Location.of("s3://test-bucket/line_separator_test/line_separator_test.csv");
        byte[] bytes = Resources.toByteArray(Resources.getResource("line_separator.csv"));
        fileSystem.newOutputFile(location).createExclusive(bytes);

        assertThat(query("SELECT * FROM TABLE(load(" +
                "location => 's3://test-bucket/line_separator_test/'," +
                "format => 'CSV'," +
                "skip_header => 1," +
                "columns => DESCRIPTOR(id VARCHAR, name VARCHAR, department VARCHAR, city VARCHAR, salary VARCHAR, join_date VARCHAR)," +
                "line_separator =>'~'))")).skippingTypesCheck()
                .matches("VALUES " +
                        "('1', 'Name1', 'Engineering', 'Bangalore', '95000', '2021-03-15')," +
                        "('2', 'Name2', 'Data Analytics', 'Mumbai', '87000', '2020-07-22')," +
                        "('3', 'Name3', 'DevOps', 'Hyderabad', '102000', '2019-11-01')," +
                        "('4', 'Name4', 'Product Management', 'Pune', '115000', '2022-01-10')," +
                        "('5', 'Name5', 'Solutions Architecture', 'Delhi', '130000', '2018-05-30')");
    }

    @Test
    void testUnsupportedFileSystem()
    {
        assertQueryFails("SELECT * FROM TABLE(load(location=>'hdfs://dummy'))", "Invalid location: hdfs://dummy");
    }

    @Test
    void testLoadCsvWithCustomSeparators()
            throws Exception
    {
        Location location = Location.of("s3://test-bucket/csv_custom_separators");
        fileSystem.createDirectory(location);

        // Use pipe as separator, double quote as quote, backslash as escape
        fileSystem.newOutputFile(location.appendPath("test.csv")).createExclusive("id|name|value\n1|\"John\\\"Doe\"|100\n2|Jane|200".getBytes(UTF_8));

        assertThat(query("SELECT * FROM TABLE(load('s3://test-bucket/csv_custom_separators/', 'CSV', DESCRIPTOR(\"id\" VARCHAR, \"name\" VARCHAR, \"value\" VARCHAR), 1, '|', '\"', '\\'))"))
                .matches("VALUES (VARCHAR '1', VARCHAR 'John\"Doe', VARCHAR '100'), ('2', 'Jane', '200')");

        assertThat(query("SELECT * FROM TABLE(load(location=>'s3://test-bucket/csv_custom_separators/', skip_header=>1, field_separator=>'|', quote_char=>'\"', escape_char=>'\\'))"))
                .matches("VALUES (VARCHAR '1', VARCHAR 'John\"Doe', VARCHAR '100'), ('2', 'Jane', '200')");
    }

    @ParameterizedTest
    @EnumSource(mode = EnumSource.Mode.EXCLUDE, names = {"REGEX", "ESRI", "ESRI_GEO_JSON", "SEQUENCEFILE_PROTOBUF"})
    void testUnload(HiveStorageFormat format)
            throws Exception
    {
        String tableName = "test_unload_" + randomNameSuffix();
        String location = "s3://%s/%s/%s".formatted("test-bucket", tableName, randomNameSuffix());

        fileSystem.createDirectory(Location.of(location));

        try {
            assertQuerySucceeds("SELECT * FROM TABLE(unload(" +
                    "input => TABLE(SELECT name, comment FROM tpch.tiny.region)," +
                    "location => '" + location + "'," +
                    "format => '" + format + "'))");

            assertUpdate("CREATE TABLE " + tableName + "(name VARCHAR, comment VARCHAR) WITH (external_location = '" + location + "', format = '" + format + "')");
            assertThat(query("SELECT * FROM " + tableName))
                    .matches("SELECT CAST(name AS VARCHAR), CAST(comment AS VARCHAR) FROM tpch.tiny.region");

            assertUpdate("DROP TABLE " + tableName);
        }
        finally {
            fileSystem.deleteDirectory(Location.of(location));
        }
    }

    private String loadTableLocation(String tableName)
    {
        return metastore.getTable("tpch", tableName).orElseThrow()
                .getStorage().getLocation();
    }

    private static Path createCredentialsFile(String minioEndpoint)
            throws IOException
    {
        String json =
                """
                {
                    "configurations": [
                        {
                            "id": "minio",
                            "location": "s3://test-bucket",
                            "configuration": {
                                "fs.s3.enabled": "true",
                                "s3.endpoint": "%s",
                                "s3.aws-access-key": "%s",
                                "s3.aws-secret-key": "%s",
                                "s3.region": "%s",
                                "s3.path-style-access": "true"
                            }
                        },
                        {
                            "id": "duplicate#1",
                            "location": "s3://test-duplicate/default",
                            "configuration": {
                                "fs.s3.enabled": "true",
                                "s3.region": "us-east-1"
                            }
                        },
                        {
                            "id": "duplicate#2",
                            "location": "s3://test-duplicate/default_prefix",
                            "configuration": {
                                "fs.s3.enabled": "true",
                                "s3.region": "us-east-1"
                            }
                        }
                    ]
                }
                """.formatted(minioEndpoint, MINIO_ROOT_USER, MINIO_ROOT_PASSWORD, MINIO_REGION).stripIndent();

        Path config = Files.createTempFile("starburst_functions", "json");
        config.toFile().deleteOnExit();
        Files.writeString(config, json.stripIndent());
        return config;
    }

    private static TrinoFileSystem fileSystem(ConnectorSession session, Minio minio)
    {
        S3FileSystemConfig config = new S3FileSystemConfig()
                .setEndpoint(minio.getMinioAddress())
                .setAwsAccessKey(MINIO_ROOT_USER)
                .setAwsSecretKey(MINIO_ROOT_PASSWORD)
                .setRegion(MINIO_REGION)
                .setPathStyleAccess(true);
        S3FileSystemFactory fileSystemFactory = new S3FileSystemFactory(OpenTelemetry.noop(), config, new S3FileSystemStats());
        return fileSystemFactory.create(session);
    }

    public static class DenyLocationAccessControlModule
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            newOptionalBinder(binder, LocationAccessControl.class)
                    .setBinding().toInstance(new DenyAccessControl());
        }
    }

    public static class DenyAccessControl
            implements LocationAccessControl
    {
        @Override
        public void checkCanUseLocation(ConnectorIdentity identity, String location)
        {
            if (location.contains("denied")) {
                throw new AccessDeniedException(location);
            }
        }
    }
}
