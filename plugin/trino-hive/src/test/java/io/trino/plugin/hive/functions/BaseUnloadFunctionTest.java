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
package io.trino.plugin.hive.functions;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.hive.HiveCompressionOption;
import io.trino.plugin.hive.HiveQueryRunner;
import io.trino.plugin.hive.HiveStorageFormat;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.EnumSource.Mode;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.hive.HiveCompressionCodecs.toCompressionCodec;
import static io.trino.plugin.hive.HiveStorageFormat.AVRO;
import static io.trino.plugin.hive.HiveStorageFormat.ESRI;
import static io.trino.plugin.hive.HiveStorageFormat.ESRI_GEO_JSON;
import static io.trino.plugin.hive.HiveStorageFormat.JSON;
import static io.trino.plugin.hive.HiveStorageFormat.OPENX_JSON;
import static io.trino.plugin.hive.HiveStorageFormat.PARQUET;
import static io.trino.plugin.hive.HiveStorageFormat.RCBINARY;
import static io.trino.plugin.hive.HiveStorageFormat.RCTEXT;
import static io.trino.plugin.hive.HiveStorageFormat.REGEX;
import static io.trino.plugin.hive.HiveStorageFormat.SEQUENCEFILE;
import static io.trino.plugin.hive.HiveStorageFormat.SEQUENCEFILE_PROTOBUF;
import static io.trino.plugin.hive.HiveStorageFormat.TEXTFILE;
import static io.trino.testing.DataProviders.cartesianProduct;
import static io.trino.testing.DataProviders.toDataProvider;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.abort;

abstract class BaseUnloadFunctionTest
        extends AbstractTestQueryFramework
{
    private Path directory;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = HiveQueryRunner.builder()
                .setHiveProperties(getAdditionalConnectorProperties())
                .addHiveProperty("parquet.writer.validation-percentage", "100")
                // unload locations are file:// URIs served by the Hadoop file system
                .addHiveProperty("fs.hadoop.enabled", "true")
                .build();
        directory = queryRunner.getCoordinator().getBaseDataDir().resolve("unload");
        Files.createDirectory(directory);
        return queryRunner;
    }

    protected Map<String, String> getAdditionalConnectorProperties()
    {
        return ImmutableMap.of();
    }

    @AfterAll
    void tearDown()
            throws Exception
    {
        deleteRecursively(directory, ALLOW_INSECURE);
    }

    @ParameterizedTest
    @EnumSource(mode = Mode.EXCLUDE, names = {"CSV", "REGEX", "ESRI", "ESRI_GEO_JSON", "SEQUENCEFILE_PROTOBUF"})
    void testUnloadFormat(HiveStorageFormat format)
            throws Exception
    {
        String tableName = "test_unload_format_" + randomNameSuffix();
        String location = directory.resolve(tableName).toUri().toString();
        Files.createDirectory(directory.resolve(tableName));

        MaterializedResult result = computeActual("SELECT * FROM TABLE(hive.system.unload(input => TABLE(tpch.tiny.region), location => '" + location + "', format => '" + format.name() + "'))");
        assertThat(result.getColumnNames()).containsExactly("path", "count");
        assertThat(result.getRowCount()).isEqualTo(1);

        assertUpdate("CREATE TABLE " + tableName + "(LIKE tpch.tiny.region) WITH (external_location = '" + location + "', format = '" + format.name() + "')");
        assertThat(query("SELECT * FROM " + tableName)).matches("SELECT * FROM tpch.tiny.region");

        assertUpdate("DROP TABLE " + tableName);
    }

    @ParameterizedTest
    @MethodSource("testUnloadCompressionSource")
    void testUnloadCompression(HiveStorageFormat format, HiveCompressionOption compression)
            throws Exception
    {
        String tableName = "test_unload_compression_" + randomNameSuffix();
        String location = directory.resolve(tableName).toUri().toString();
        Files.createDirectory(directory.resolve(tableName));

        String unload = "SELECT * FROM TABLE(hive.system.unload(" +
                "input => TABLE(VALUES (CAST('1' AS varchar), CAST('a' AS varchar)), (CAST('2' AS varchar), CAST('b' AS varchar))) t(id, data)," +
                "location => '" + location + "'," +
                "format => '" + format.name() + "'," +
                "compression => '" + compression.name() + "'))";

        if ((format == PARQUET) && compression == HiveCompressionOption.LZ4) {
            assertThat(query(unload))
                    .nonTrinoExceptionFailure().hasMessageMatching("Unsupported compression codec for Parquet: LZ4");
            abort();
        }

        MaterializedResult result = computeActual(unload);
        assertThat(result.getColumnNames()).containsExactly("path", "count");
        assertThat(result.getRowCount()).isEqualTo(1);

        assertUpdate("CREATE TABLE " + tableName + "(id varchar, data varchar) WITH (external_location = '" + location + "', format = '" + format + "')");
        assertQuery("SELECT * FROM " + tableName, "VALUES ('1', 'a'), ('2', 'b')");

        assertUpdate("DROP TABLE " + tableName);
    }

    public static Object[][] testUnloadCompressionSource()
    {
        return cartesianProduct(
                Stream.of(HiveStorageFormat.values())
                        .filter(format -> format != REGEX)
                        .filter(format -> format != ESRI)
                        .filter(format -> format != ESRI_GEO_JSON)
                        .filter(format -> format != SEQUENCEFILE_PROTOBUF)
                        .collect(toDataProvider()),
                Stream.of(HiveCompressionOption.values())
                        .collect(toDataProvider()));
    }

    @Test
    void testUnloadTextfileSeparator()
            throws Exception
    {
        String tableName = "test_unload_textfile_separator_" + randomNameSuffix();
        String location = directory.resolve(tableName).toUri().toString();
        Files.createDirectory(directory.resolve(tableName));

        MaterializedResult result = computeActual("SELECT * FROM TABLE(hive.system.unload(" +
                "input => TABLE(tpch.tiny.region)," +
                "location => '" + location + "'," +
                "format => 'TEXTFILE'," +
                "separator => '|'))");
        assertThat(result.getColumnNames()).containsExactly("path", "count");
        assertThat(result.getRowCount()).isEqualTo(1);

        assertUpdate("CREATE TABLE " + tableName + "(LIKE tpch.tiny.region) WITH (external_location = '" + location + "', format = 'TEXTFILE', textfile_field_separator = '|')");
        assertThat(query("SELECT * FROM " + tableName)).matches("SELECT * FROM tpch.tiny.region");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    void testUnloadTextfileCompressionAndSeparator()
            throws Exception
    {
        String tableName = "test_unload_textfile_compression_separator_" + randomNameSuffix();
        String location = directory.resolve(tableName).toUri().toString();
        Files.createDirectory(directory.resolve(tableName));

        MaterializedResult result = computeActual("SELECT * FROM TABLE(hive.system.unload(" +
                "input => TABLE(tpch.tiny.region)," +
                "location => '" + location + "'," +
                "format => 'TEXTFILE'," +
                "compression => 'GZIP'," +
                "separator => '#'))");
        assertThat(result.getColumnNames()).containsExactly("path", "count");
        assertThat(result.getRowCount()).isEqualTo(1);

        assertUpdate("CREATE TABLE " + tableName + "(LIKE tpch.tiny.region) WITH (external_location = '" + location + "', format = 'TEXTFILE', textfile_field_separator = '#')");
        assertThat(query("SELECT * FROM " + tableName)).matches("SELECT * FROM tpch.tiny.region");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    void testUnloadTextfileHeader()
            throws Exception
    {
        String tableName = "test_unload_textfile_header_" + randomNameSuffix();
        String location = directory.resolve(tableName).toUri().toString();
        Files.createDirectory(directory.resolve(tableName));

        MaterializedResult result = computeActual("SELECT * FROM TABLE(hive.system.unload(" +
                "input => TABLE(VALUES (1, 'a'), (2, 'b')) t(id, data)," +
                "location => '" + location + "'," +
                "format => 'TEXTFILE'," +
                "header => true))");
        assertThat(result.getColumnNames()).containsExactly("path", "count");
        assertThat(result.getRowCount()).isEqualTo(1);

        assertThat(Files.readString(Path.of(URI.create((String) result.getMaterializedRows().getFirst().getField(0)))))
                .isEqualTo(
                        """
                        iddata
                        1a
                        2b
                        """);

        assertUpdate("CREATE TABLE " + tableName + "(id integer, data varchar) WITH (external_location = '" + location + "', format = 'TEXTFILE', skip_header_line_count = 1)");
        assertQuery("SELECT * FROM " + tableName, "VALUES ('1', 'a'), ('2', 'b')");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    void testUnloadTextfileHeaderFalse()
            throws Exception
    {
        String tableName = "test_unload_textfile_header_" + randomNameSuffix();
        String location = directory.resolve(tableName).toUri().toString();
        Files.createDirectory(directory.resolve(tableName));

        MaterializedResult result = computeActual("SELECT * FROM TABLE(hive.system.unload(" +
                "input => TABLE(VALUES (1, 'a'), (2, 'b')) t(id, data)," +
                "location => '" + location + "'," +
                "format => 'TEXTFILE'," +
                "header => false))");
        assertThat(result.getColumnNames()).containsExactly("path", "count");
        assertThat(result.getRowCount()).isEqualTo(1);

        assertThat(Files.readString(Path.of(URI.create((String) result.getMaterializedRows().getFirst().getField(0)))))
                .isEqualTo(
                        """
                        1a
                        2b
                        """);

        assertUpdate("CREATE TABLE " + tableName + "(id integer, data varchar) WITH (external_location = '" + location + "', format = 'TEXTFILE')");
        assertQuery("SELECT * FROM " + tableName, "VALUES ('1', 'a'), ('2', 'b')");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    void testUnloadCsvSeparator()
            throws Exception
    {
        String tableName = "test_unload_csv_separator_" + randomNameSuffix();
        String location = directory.resolve(tableName).toUri().toString();
        Files.createDirectory(directory.resolve(tableName));

        MaterializedResult result = computeActual("SELECT * FROM TABLE(hive.system.unload(" +
                "input => TABLE(VALUES (CAST('1' AS varchar), CAST('a' AS varchar)), (CAST('2' AS varchar), CAST('b' AS varchar))) t(id, data)," +
                "location => '" + location + "'," +
                "format => 'CSV'," +
                "separator => '#'))");
        assertThat(result.getColumnNames()).containsExactly("path", "count");
        assertThat(result.getRowCount()).isEqualTo(1);

        assertUpdate("CREATE TABLE " + tableName + "(id varchar, data varchar) WITH (external_location = '" + location + "', format = 'CSV', csv_separator = '#')");
        assertQuery("SELECT * FROM " + tableName, "VALUES ('1', 'a'), ('2', 'b')");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    void testUnloadCsvCompressionAndSeparator()
            throws Exception
    {
        String tableName = "test_unload_csv_compression_separator_" + randomNameSuffix();
        String location = directory.resolve(tableName).toUri().toString();
        Files.createDirectory(directory.resolve(tableName));

        MaterializedResult result = computeActual("SELECT * FROM TABLE(hive.system.unload(" +
                "input => TABLE(VALUES (CAST('1' AS varchar), CAST('a' AS varchar)), (CAST('2' AS varchar), CAST('b' AS varchar))) t(id, data)," +
                "location => '" + location + "'," +
                "format => 'CSV'," +
                "compression => 'GZIP'," +
                "separator => '|'))");
        assertThat(result.getColumnNames()).containsExactly("path", "count");
        assertThat(result.getRowCount()).isEqualTo(1);

        assertUpdate("CREATE TABLE " + tableName + "(id varchar, data varchar) WITH (external_location = '" + location + "', format = 'CSV', csv_separator = '|')");
        assertQuery("SELECT * FROM " + tableName, "VALUES ('1', 'a'), ('2', 'b')");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    void testUnloadCsvHeader()
            throws Exception
    {
        String tableName = "test_unload_csv_header_" + randomNameSuffix();
        String location = directory.resolve(tableName).toUri().toString();
        Files.createDirectory(directory.resolve(tableName));

        MaterializedResult result = computeActual("SELECT * FROM TABLE(hive.system.unload(" +
                "input => TABLE(VALUES (CAST('1' AS varchar), CAST('a' AS varchar)), (CAST('2' AS varchar), CAST('b' AS varchar))) t(id, data)," +
                "location => '" + location + "'," +
                "format => 'CSV'," +
                "header => true))");
        assertThat(result.getColumnNames()).containsExactly("path", "count");
        assertThat(result.getRowCount()).isEqualTo(1);

        assertThat(Files.readString(Path.of(URI.create((String) result.getMaterializedRows().getFirst().getField(0)))))
                .isEqualTo(
                        """
                        "id","data"
                        "1","a"
                        "2","b"
                        """);

        assertUpdate("CREATE TABLE " + tableName + "(id varchar, data varchar) WITH (external_location = '" + location + "', format = 'CSV', skip_header_line_count = 1)");
        assertQuery("SELECT * FROM " + tableName, "VALUES ('1', 'a'), ('2', 'b')");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    void testUnloadCsvHeaderFalse()
            throws Exception
    {
        String tableName = "test_unload_csv_header_" + randomNameSuffix();
        String location = directory.resolve(tableName).toUri().toString();
        Files.createDirectory(directory.resolve(tableName));

        MaterializedResult result = computeActual("SELECT * FROM TABLE(hive.system.unload(" +
                "input => TABLE(VALUES (CAST('1' AS varchar), CAST('a' AS varchar)), (CAST('2' AS varchar), CAST('b' AS varchar))) t(id, data)," +
                "location => '" + location + "'," +
                "format => 'CSV'," +
                "header => false))");
        assertThat(result.getColumnNames()).containsExactly("path", "count");
        assertThat(result.getRowCount()).isEqualTo(1);

        assertThat(Files.readString(Path.of(URI.create((String) result.getMaterializedRows().getFirst().getField(0)))))
                .isEqualTo(
                        """
                        "1","a"
                        "2","b"
                        """);

        assertUpdate("CREATE TABLE " + tableName + "(id varchar, data varchar) WITH (external_location = '" + location + "', format = 'CSV')");
        assertQuery("SELECT * FROM " + tableName, "VALUES ('1', 'a'), ('2', 'b')");
        assertUpdate("DROP TABLE " + tableName);
    }

    @ParameterizedTest
    @MethodSource("testUnloadColumnNamesSource")
    void testUnloadColumnNames(HiveStorageFormat format, String columnName)
            throws Exception
    {
        if (columnName.equals("atrailingspace ")) {
            assertThatThrownBy(() -> testUnloadColumnName(format, columnName))
                    .hasMessageContaining("Hive column names must not end with a space")
                    .hasStackTraceContaining("SELECT * FROM TABLE(hive.system.unload");
            abort();
        }
        if (columnName.equals(" aleadingspace")) {
            assertThatThrownBy(() -> testUnloadColumnName(format, columnName))
                    .hasMessageContaining("Hive column names must not start with a space")
                    .hasStackTraceContaining("SELECT * FROM TABLE(hive.system.unload");
            abort();
        }
        if (columnName.equals("a,comma")) {
            assertThatThrownBy(() -> testUnloadColumnName(format, columnName))
                    .hasMessageContaining("Hive column names must not contain commas")
                    .hasStackTraceContaining("SELECT * FROM TABLE(hive.system.unload");
            abort();
        }
        if (format == AVRO && !(columnName.equals("lowercase") ||
                columnName.equals("UPPERCASE") ||
                columnName.equals("MixedCase") ||
                columnName.equals("an_underscore") ||
                columnName.equals("adigit0"))) {
            assertThatThrownBy(() -> testUnloadColumnName(format, columnName))
                    .hasMessageContaining("AVRO format does not support the definition")
                    .hasStackTraceContaining("SELECT * FROM TABLE(hive.system.unload");
            abort();
        }
        testUnloadColumnName(format, columnName);
    }

    void testUnloadColumnName(HiveStorageFormat format, String columnName)
            throws Exception
    {
        String tableName = "test_unload_column_name_" + randomNameSuffix();
        String columnNameInSql = toColumnNameInSql(columnName, requiresDelimiting(columnName));
        String location = directory.resolve(tableName).toUri().toString();
        Files.createDirectory(directory.resolve(tableName));

        MaterializedResult result = computeActual("SELECT * FROM TABLE(hive.system.unload(" +
                "input => TABLE(SELECT CAST('x' AS VARCHAR) AS " + columnNameInSql + ")," +
                "location => '" + location + "'," +
                "format => '" + format.name() + "'))");
        assertThat(result.getColumnNames()).containsExactly("path", "count");
        assertThat(result.getRowCount()).isEqualTo(1);

        assertUpdate("CREATE TABLE " + tableName + "(" + columnNameInSql + " varchar) WITH (external_location = '" + location + "', format = '" + format.name() + "')");
        assertQuery("SELECT * FROM " + tableName, "VALUES 'x'");

        assertUpdate("DROP TABLE " + tableName);
    }

    public static Object[][] testUnloadColumnNamesSource()
    {
        return cartesianProduct(
                Stream.of(HiveStorageFormat.values())
                        .filter(format -> format != REGEX)
                        .filter(format -> format != ESRI)
                        .filter(format -> format != ESRI_GEO_JSON)
                        .filter(format -> format != SEQUENCEFILE_PROTOBUF)
                        .collect(toDataProvider()),
                new Object[][] {
                        {"lowercase"},
                        {"UPPERCASE"},
                        {"MixedCase"},
                        {"an_underscore"},
                        {"a-hyphen-minus"}, // ASCII '-' is HYPHEN-MINUS in Unicode
                        {"a space"},
                        {"atrailingspace "},
                        {" aleadingspace"},
                        {"a.dot"},
                        {"a,comma"},
                        {"a:colon"},
                        {"a;semicolon"},
                        {"an@at"},
                        {"a\"quote"},
                        {"an'apostrophe"},
                        {"a`backtick`"},
                        {"a/slash`"},
                        {"a\\backslash`"},
                        {"adigit0"},
                        {"0startwithdigit"},
                });
    }

    protected static boolean requiresDelimiting(String identifierName)
    {
        return !identifierName.matches("[a-zA-Z][a-zA-Z0-9_]*");
    }

    private static String toColumnNameInSql(String columnName, boolean delimited)
    {
        String nameInSql = columnName;
        if (delimited) {
            nameInSql = "\"" + columnName.replace("\"", "\"\"") + "\"";
        }
        return nameInSql;
    }

    @Test
    void testUnloadPartition()
            throws Exception
    {
        String tableName = "test_unload_partition_" + randomNameSuffix();
        String location = directory.resolve(tableName).toUri().toString();
        Files.createDirectory(directory.resolve(tableName));

        MaterializedResult result = computeActual("SELECT * FROM TABLE(hive.system.unload(" +
                "input => TABLE(VALUES (1, 'part1'), (2, 'part2'), (20, 'part2'), (3, 'part3'), (30, 'part3'), (300, 'part3')) t(data, part) PARTITION BY part," +
                "location => '" + location + "'," +
                "format => 'ORC'))");

        assertThat(result.getColumnNames()).containsExactly("path", "count", "part");

        // Create partitioned table, sync partitions and verify results
        assertUpdate("CREATE TABLE " + tableName + "(data bigint, part varchar) WITH (external_location = '" + location + "', format = 'ORC', partitioned_by = ARRAY['part'])");
        assertQueryReturnsEmptyResult("SELECT * FROM " + tableName);

        assertUpdate("CALL system.sync_partition_metadata('tpch', '" + tableName + "', 'ADD')");
        assertQuery(
                "SELECT * FROM \"" + tableName + "$partitions\"",
                "VALUES 'part1', 'part2', 'part3'");
        assertQuery(
                "SELECT * FROM " + tableName,
                "VALUES (1, 'part1'), (2, 'part2'), (20, 'part2'), (3, 'part3'), (30, 'part3'), (300, 'part3')");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    void testUnloadMultiplePartitionKeys()
            throws Exception
    {
        String tableName = "test_unload_partitions_" + randomNameSuffix();
        String location = directory.resolve(tableName).toUri().toString();
        Files.createDirectory(directory.resolve(tableName));

        MaterializedResult result = computeActual("SELECT * FROM TABLE(hive.system.unload(" +
                "input => TABLE(VALUES" +
                " (1, 'part1', 'subpart1')," +
                " (2, 'part2', 'subpart1'), (20, 'part2', 'subpart2')," +
                " (3, 'part3', 'subpart1'), (30, 'part3', 'subpart2'), (300, 'part3', 'subpart3'))" +
                "t(data, part, subpart) PARTITION BY (part, subpart)," +
                "location => '" + location + "'," +
                "format => 'ORC'))");

        assertThat(result.getColumnNames()).containsExactly("path", "count", "part", "subpart");

        // Create partitioned table, sync partitions and verify results
        assertUpdate("CREATE TABLE " + tableName + "(data bigint, part varchar, subpart varchar)" +
                "WITH (external_location = '" + location + "', format = 'ORC', partitioned_by = ARRAY['part', 'subpart'])");
        assertQueryReturnsEmptyResult("SELECT * FROM " + tableName);

        assertUpdate("CALL system.sync_partition_metadata('tpch', '" + tableName + "', 'ADD')");
        assertQuery(
                "SELECT * FROM \"" + tableName + "$partitions\"",
                "VALUES ('part1', 'subpart1'), ('part2', 'subpart1'), ('part2', 'subpart2'), ('part3', 'subpart1'), ('part3', 'subpart2'), ('part3', 'subpart3')");
        assertQuery(
                "SELECT * FROM " + tableName,
                "VALUES" +
                        "(1, 'part1', 'subpart1')," +
                        "(2, 'part2', 'subpart1'), (20, 'part2', 'subpart2')," +
                        "(3, 'part3', 'subpart1'), (30, 'part3', 'subpart2'), (300, 'part3', 'subpart3')");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    void testUnloadNonLowercasePartitionKey()
            throws Exception
    {
        String tableName = "test_unload_case_partition_" + randomNameSuffix();
        String location = directory.resolve(tableName).toUri().toString();
        Files.createDirectory(directory.resolve(tableName));

        MaterializedResult result = computeActual("SELECT * FROM TABLE(hive.system.unload(" +
                "input => TABLE(VALUES (1, 'part1'), (2, 'part2'), (20, 'part2'), (3, 'part3'), (30, 'part3'), (300, 'part3')) t(data, PART) PARTITION BY \"PART\"," +
                "location => '" + location + "'," +
                "format => 'ORC'))");

        assertThat(result.getColumnNames()).containsExactly("path", "count", "PART");

        // Create partitioned table, sync partitions and verify results
        assertUpdate("CREATE TABLE " + tableName + "(data bigint, part varchar) WITH (external_location = '" + location + "', format = 'ORC', partitioned_by = ARRAY['part'])");
        assertQueryReturnsEmptyResult("SELECT * FROM " + tableName);

        assertUpdate("CALL system.sync_partition_metadata('tpch', '" + tableName + "', 'ADD')");
        assertQuery(
                "SELECT * FROM \"" + tableName + "$partitions\"",
                "VALUES 'part1', 'part2', 'part3'");
        assertQuery(
                "SELECT * FROM " + tableName,
                "VALUES (1, 'part1'), (2, 'part2'), (20, 'part2'), (3, 'part3'), (30, 'part3'), (300, 'part3')");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    void testUnloadLargeResult()
            throws Exception
    {
        String tableName = "test_unload_large_result_" + randomNameSuffix();
        String location = directory.resolve(tableName).toUri().toString();
        Files.createDirectory(directory.resolve(tableName));

        MaterializedResult result = computeActual("SELECT * FROM TABLE(hive.system.unload(" +
                "input => TABLE(tpch.sf1.orders)," +
                "location => '" + location + "'," +
                "format => 'ORC'))");

        assertThat(result.getColumnNames()).containsExactly("path", "count");
        assertThat(result.getRowCount()).isGreaterThanOrEqualTo(minFilesCreated());
        long actual = result.project("count").getOnlyColumn().mapToLong(n -> (long) n).sum();
        assertThat(actual).isEqualTo(1500000L);

        assertUpdate("CREATE TABLE " + tableName + "(LIKE tpch.sf1.orders) WITH (external_location = '" + location + "', format = 'ORC')");
        assertThat(query("SELECT count(1) FROM " + tableName)).matches("SELECT count(1) FROM tpch.sf1.orders");

        assertUpdate("DROP TABLE " + tableName);
    }

    protected abstract int minFilesCreated();

    @Test
    void testUnloadPartitionLargeResult()
            throws Exception
    {
        String tableName = "test_unload_partition_large_result_" + randomNameSuffix();
        String location = directory.resolve(tableName).toUri().toString();
        Files.createDirectory(directory.resolve(tableName));

        MaterializedResult result = computeActual("SELECT * FROM TABLE(hive.system.unload(" +
                "input => TABLE(SELECT orderkey, custkey, orderstatus FROM tpch.sf1.orders) PARTITION BY (orderstatus)," +
                "location => '" + location + "'," +
                "format => 'ORC'))");

        assertThat(result.getColumnNames()).containsExactly("path", "count", "orderstatus");
        assertThat(result.getRowCount()).isEqualTo(3);
        long actual = result.project("count").getOnlyColumn().mapToLong(n -> (long) n).sum();
        assertThat(actual).isEqualTo(1500000L);

        assertUpdate("CREATE TABLE " + tableName + "(" +
                "orderkey bigint," +
                "custkey bigint," +
                "orderstatus varchar(1)" +
                ") WITH (" +
                "external_location = '" + location + "'," +
                "format = 'ORC'," +
                "partitioned_by = ARRAY['orderstatus'])");
        assertUpdate("CALL system.sync_partition_metadata('tpch', '" + tableName + "', 'ADD')");
        assertThat(query("SELECT count(1) FROM " + tableName)).matches("SELECT count(1) FROM tpch.sf1.orders");

        assertUpdate("DROP TABLE " + tableName);
    }

    @ParameterizedTest
    @EnumSource(mode = Mode.EXCLUDE, names = {"CSV", "REGEX", "ESRI", "ESRI_GEO_JSON", "SEQUENCEFILE_PROTOBUF"})
    void testUnloadBoolean(HiveStorageFormat format)
            throws Exception
    {
        testUnloadColumnType(format, "boolean", "true");
        testUnloadColumnType(format, "boolean", "false");
        testUnloadColumnType(format, "boolean", "NULL");
    }

    @ParameterizedTest
    @EnumSource(mode = Mode.EXCLUDE, names = {"AVRO", "CSV", "REGEX", "ESRI", "ESRI_GEO_JSON", "SEQUENCEFILE_PROTOBUF"})
    // see testUnloadAvro
    void testUnloadTinyInt(HiveStorageFormat format)
            throws Exception
    {
        testUnloadColumnType(format, "tinyint", "-128");
        testUnloadColumnType(format, "tinyint", "127");
        testUnloadColumnType(format, "tinyint", "NULL");
    }

    @ParameterizedTest
    @EnumSource(mode = Mode.EXCLUDE, names = {"AVRO", "CSV", "REGEX", "ESRI", "ESRI_GEO_JSON", "SEQUENCEFILE_PROTOBUF"})
    // see testUnloadAvro
    void testUnloadSmallInt(HiveStorageFormat format)
            throws Exception
    {
        testUnloadColumnType(format, "smallint", "-32768");
        testUnloadColumnType(format, "smallint", "32767");
        testUnloadColumnType(format, "smallint", "NULL");
    }

    @ParameterizedTest
    @EnumSource(mode = Mode.EXCLUDE, names = {"CSV", "REGEX", "ESRI", "ESRI_GEO_JSON", "SEQUENCEFILE_PROTOBUF"})
    void testUnloadInteger(HiveStorageFormat format)
            throws Exception
    {
        testUnloadColumnType(format, "integer", "-2147483648");
        testUnloadColumnType(format, "integer", "2147483647");
        testUnloadColumnType(format, "integer", "NULL");
    }

    @ParameterizedTest
    @EnumSource(mode = Mode.EXCLUDE, names = {"CSV", "REGEX", "ESRI", "ESRI_GEO_JSON", "SEQUENCEFILE_PROTOBUF"})
    void testUnloadBigInt(HiveStorageFormat format)
            throws Exception
    {
        testUnloadColumnType(format, "bigint", "-9223372036854775808");
        testUnloadColumnType(format, "bigint", "9223372036854775807");
        testUnloadColumnType(format, "bigint", "NULL");
    }

    @ParameterizedTest
    @EnumSource(mode = Mode.EXCLUDE, names = {"CSV", "REGEX", "ESRI", "ESRI_GEO_JSON", "SEQUENCEFILE_PROTOBUF"})
    void testUnloadReal(HiveStorageFormat format)
            throws Exception
    {
        testUnloadColumnType(format, "real", "3.14");
        testUnloadColumnType(format, "real", "10.3e0");
        if (format == JSON) {
            assertThatThrownBy(() -> testUnloadColumnType(format, "real", "nan()")).hasMessageContaining("Invalid value to Insert");
            assertThatThrownBy(() -> testUnloadColumnType(format, "real", "-infinity()")).hasMessageContaining("Invalid value to Insert");
            assertThatThrownBy(() -> testUnloadColumnType(format, "real", "+infinity()")).hasMessageContaining("Invalid value to Insert");
        }
        else {
            testUnloadColumnType(format, "real", "nan()");
            testUnloadColumnType(format, "real", "-infinity()");
            testUnloadColumnType(format, "real", "+infinity()");
        }
        testUnloadColumnType(format, "real", "NULL");
    }

    @ParameterizedTest
    @EnumSource(mode = Mode.EXCLUDE, names = {"CSV", "REGEX", "ESRI", "ESRI_GEO_JSON", "SEQUENCEFILE_PROTOBUF"})
    void testUnloadDouble(HiveStorageFormat format)
            throws Exception
    {
        testUnloadColumnType(format, "double", "3.14");
        testUnloadColumnType(format, "double", "1.0E100");
        testUnloadColumnType(format, "double", "1.23456E12");
        if (format == JSON) {
            assertThatThrownBy(() -> testUnloadColumnType(format, "double", "nan()")).hasMessageContaining("Invalid value to Insert");
            assertThatThrownBy(() -> testUnloadColumnType(format, "double", "-infinity()")).hasMessageContaining("Invalid value to Insert");
            assertThatThrownBy(() -> testUnloadColumnType(format, "double", "+infinity()")).hasMessageContaining("Invalid value to Insert");
        }
        else {
            testUnloadColumnType(format, "double", "nan()");
            testUnloadColumnType(format, "double", "-infinity()");
            testUnloadColumnType(format, "double", "+infinity()");
        }
        testUnloadColumnType(format, "double", "NULL");
    }

    @ParameterizedTest
    @EnumSource(mode = Mode.EXCLUDE, names = {"CSV", "REGEX", "ESRI", "ESRI_GEO_JSON", "SEQUENCEFILE_PROTOBUF"})
    void testUnloadDecimal(HiveStorageFormat format)
            throws Exception
    {
        testUnloadColumnType(format, "decimal(3, 0)", "193");
        testUnloadColumnType(format, "decimal(3, 1)", "10.1");
        testUnloadColumnType(format, "decimal(24, 2)", "2");
        testUnloadColumnType(format, "decimal(24, 2)", "12345678901234567890.31");
        testUnloadColumnType(format, "decimal(38, 0)", "'27182818284590452353602874713526624977'");
        testUnloadColumnType(format, "decimal(3, 0)", "NULL");
        testUnloadColumnType(format, "decimal(38, 0)", "NULL");
    }

    @ParameterizedTest
    @EnumSource(mode = Mode.EXCLUDE, names = {"CSV", "REGEX", "ESRI", "ESRI_GEO_JSON", "SEQUENCEFILE_PROTOBUF"})
    void testUnloadChar(HiveStorageFormat format)
            throws Exception
    {
        testUnloadColumnType(format, "char", "''");
        testUnloadColumnType(format, "char", "'a'");
        testUnloadColumnType(format, "char(1)", "''");
        testUnloadColumnType(format, "char(1)", "'a'");
        testUnloadColumnType(format, "char(1)", "'😂'");
        testUnloadColumnType(format, "char(8)", "'abc'");
        testUnloadColumnType(format, "char(8)", "'12345678'");
        testUnloadColumnType(format, "char(10)", "NULL");
        testUnloadColumnType(format, "char", "NULL");
    }

    @ParameterizedTest
    @EnumSource(mode = Mode.EXCLUDE, names = {"CSV", "REGEX", "ESRI", "ESRI_GEO_JSON", "SEQUENCEFILE_PROTOBUF"})
    void testUnloadVarchar(HiveStorageFormat format)
            throws Exception
    {
        testUnloadColumnType(format, "varchar(10)", "'text_a'");
        testUnloadColumnType(format, "varchar(255)", "'text_b'");
        testUnloadColumnType(format, "varchar(65535)", "'text_c'");
        testUnloadColumnType(format, "varchar(5)", "'攻殻機動隊'");
        testUnloadColumnType(format, "varchar(32)", "'攻殻機動隊'");
        testUnloadColumnType(format, "varchar(20000)", "'攻殻機動隊'");
        testUnloadColumnType(format, "varchar(1)", "'😂'");
        testUnloadColumnType(format, "varchar(77)", "'Ну, погоди!'");
        testUnloadColumnType(format, "varchar(10)", "NULL");
    }

    @ParameterizedTest
    @EnumSource(mode = Mode.EXCLUDE, names = {"REGEX", "ESRI", "ESRI_GEO_JSON", "SEQUENCEFILE_PROTOBUF"})
    void testUnloadUnboundedVarchar(HiveStorageFormat format)
            throws Exception
    {
        testUnloadColumnType(format, "varchar", "'text_a'");
        testUnloadColumnType(format, "varchar", "'text_b'");
        testUnloadColumnType(format, "varchar", "'text_c'");
        testUnloadColumnType(format, "varchar", "'攻殻機動隊'");
        testUnloadColumnType(format, "varchar", "'攻殻機動隊'");
        testUnloadColumnType(format, "varchar", "'攻殻機動隊'");
        testUnloadColumnType(format, "varchar", "'😂'");
        testUnloadColumnType(format, "varchar", "'Ну, погоди!'");
        testUnloadColumnType(format, "varchar", "NULL");
    }

    @ParameterizedTest
    @EnumSource(mode = Mode.EXCLUDE, names = {"CSV", "REGEX", "ESRI", "ESRI_GEO_JSON", "SEQUENCEFILE_PROTOBUF"})
    void testUnloadVarbinary(HiveStorageFormat format)
            throws Exception
    {
        if (format == JSON) {
            assertThatThrownBy(() -> testUnloadColumnType(format, "varbinary", "X''"))
                    .hasMessageContaining("UNLOAD table function does not support VARBINARY columns for JSON format");
            abort();
        }

        if (format == RCBINARY) {
            assertThatThrownBy(() -> testUnloadColumnType(format, "varbinary", "X''"))
                    .hasMessage("RCBinary encoder does not support empty VARBINARY values (HIVE-2483). Use ORC or Parquet format instead.");
        }
        else {
            testUnloadColumnType(format, "varbinary", "X''");
        }
        testUnloadColumnType(format, "varbinary", "X'68656C6C6F'");
        testUnloadColumnType(format, "varbinary", "X'5069C4996B6E6120C582C4856B61207720E69DB1E4BAACE983BD'");
        testUnloadColumnType(format, "varbinary", "X'4261672066756C6C206F6620F09F92B0'");
        testUnloadColumnType(format, "varbinary", "X'0001020304050607080DF9367AA7000000'");
        testUnloadColumnType(format, "varbinary", "X'000000000000'");
        testUnloadColumnType(format, "varbinary", "NULL");
    }

    @ParameterizedTest
    @EnumSource(mode = Mode.EXCLUDE, names = {"CSV", "REGEX", "ESRI", "ESRI_GEO_JSON", "SEQUENCEFILE_PROTOBUF"})
    void testUnloadDate(HiveStorageFormat format)
            throws Exception
    {
        testUnloadColumnType(format, "date", "'0001-01-01'");
        testUnloadColumnType(format, "date", "'1582-10-04'");
        testUnloadColumnType(format, "date", "'1582-10-05'");
        testUnloadColumnType(format, "date", "'1582-10-14'");
        testUnloadColumnType(format, "date", "'9999-12-31'");
        if (format != OPENX_JSON) {
            // no longer works with OPENX_JSON after "Use new parseHiveDate for OpenX reader to remove any characters after yyyy-mm-dd"
            testUnloadColumnType(format, "date", "'5874897-12-31'");
        }
        testUnloadColumnType(format, "date", "NULL");
    }

    @ParameterizedTest
    @EnumSource(mode = Mode.EXCLUDE, names = {"CSV", "REGEX", "ESRI", "ESRI_GEO_JSON", "SEQUENCEFILE_PROTOBUF"})
    void testUnloadTimestampMillis(HiveStorageFormat format)
            throws Exception
    {
        if (format == AVRO || format == RCBINARY) {
            assertThatThrownBy(() -> testUnloadColumnType(format, "timestamp(3)", "'1970-01-01 00:00:00'"))
                    .hasMessageContaining("UNLOAD table function does not support timestamp columns");
            abort();
        }

        // The epoch is tested in testParquetEpochTimestamp
        if (format != PARQUET) {
            testUnloadColumnType(format, "timestamp", "'1970-01-01 00:00:00'");
            testUnloadColumnType(format, "timestamp(3)", "'1970-01-01 00:00:00'");
        }

        testUnloadColumnType(format, "timestamp(3)", "'2024-10-17 12:34:56.123'");
        testUnloadColumnType(format, "timestamp", "NULL");
    }

    @ParameterizedTest
    @EnumSource(mode = Mode.EXCLUDE, names = {"CSV", "REGEX", "ESRI", "ESRI_GEO_JSON", "SEQUENCEFILE_PROTOBUF"})
    void testUnloadTimestampMicros(HiveStorageFormat format)
            throws Exception
    {
        Session session = Session.builder(getSession())
                .setSystemProperty("hive.timestamp_precision", "MICROSECONDS")
                .build();

        if (format == AVRO || format == RCBINARY) {
            assertThatThrownBy(() -> testUnloadColumnType(session, format, "timestamp(6)", "'1970-01-01 00:00:00'"))
                    .hasMessageContaining("UNLOAD table function does not support timestamp columns");
            abort();
        }

        // The epoch is tested in testParquetEpochTimestamp
        if (format != PARQUET) {
            testUnloadColumnType(session, format, "timestamp(6)", "'1970-01-01 00:00:00'");
        }
        testUnloadColumnType(session, format, "timestamp(6)", "'2024-10-17 12:34:56.123456'");
        testUnloadColumnType(session, format, "timestamp(6)", "NULL");
    }

    @ParameterizedTest
    @EnumSource(mode = Mode.EXCLUDE, names = {"CSV", "REGEX", "ESRI", "ESRI_GEO_JSON", "SEQUENCEFILE_PROTOBUF"})
    void testUnloadTimestampNanos(HiveStorageFormat format)
            throws Exception
    {
        Session session = Session.builder(getSession())
                .setSystemProperty("hive.timestamp_precision", "NANOSECONDS")
                .build();

        if (format == AVRO || format == RCBINARY) {
            assertThatThrownBy(() -> testUnloadColumnType(session, format, "timestamp(9)", "'1970-01-01 00:00:00'"))
                    .hasMessageContaining("UNLOAD table function does not support timestamp columns");
            abort();
        }

        // The epoch is tested in testParquetEpochTimestamp
        if (format != PARQUET) {
            testUnloadColumnType(session, format, "timestamp(9)", "'1970-01-01 00:00:00'");
        }
        testUnloadColumnType(session, format, "timestamp(9)", "'2024-10-17 12:34:56.123456789'");
        testUnloadColumnType(session, format, "timestamp(9)", "NULL");
    }

    @ParameterizedTest
    @CsvSource({"3, MILLISECONDS", "6, MICROSECONDS", "9, NANOSECONDS"})
    void testParquetEpochTimestamp(int precision, String precisionName)
    {
        Session session = Session.builder(getSession())
                .setSystemProperty("hive.timestamp_precision", precisionName)
                .build();

        String input = "CAST('1932-04-01 00:00:00' AS timestamp(" + precision + "))";

        // The failure occurs during daylight savings time gap for America/Bahia_Banderas timezone because
        // timestamps in that hour don't round trip convertLocalToUTC/convertUTCToLocal
        assertQueryFails(session, "CREATE TABLE test_parquet_epoch WITH (format = 'PARQUET') AS SELECT " + input + " AS a", ".*Malformed Parquet file.*");
        assertThatThrownBy(() -> testUnloadColumnType(session, PARQUET, "timestamp(" + precision + ")", input))
                .hasMessageContaining("Malformed Parquet file");
    }

    @ParameterizedTest
    @EnumSource(mode = Mode.EXCLUDE, names = {"CSV", "REGEX", "ESRI", "ESRI_GEO_JSON", "SEQUENCEFILE_PROTOBUF"})
    void testUnloadArray(HiveStorageFormat format)
            throws Exception
    {
        testUnloadColumnType(format, "array(int)", "ARRAY[123]");
        testUnloadColumnType(format, "array(varchar)", "ARRAY['test']");
        if (format == TEXTFILE || format == RCTEXT || format == SEQUENCEFILE) {
            assertThatThrownBy(() -> testUnloadColumnType(format, "array(varchar)", "ARRAY[NULL]"))
                    .hasMessageContaining(
                            """
                            Expecting actual:
                              (null)
                            to contain exactly in any order:
                              [([null])]
                            """);
            abort();
        }
        else {
            testUnloadColumnType(format, "array(varchar)", "ARRAY[NULL]");
        }
        testUnloadColumnType(format, "array(int)", "NULL");
    }

    @ParameterizedTest
    @EnumSource(mode = Mode.EXCLUDE, names = {"CSV", "REGEX", "ESRI", "ESRI_GEO_JSON", "SEQUENCEFILE_PROTOBUF"})
    void testUnloadMap(HiveStorageFormat format)
            throws Exception
    {
        testUnloadColumnType(format, "map(varchar, int)", "MAP(ARRAY[1], ARRAY[10])");
        testUnloadColumnType(format, "map(varchar, varchar)", "MAP(ARRAY['key'], ARRAY['value'])");
        testUnloadColumnType(format, "map(varchar, int)", "MAP(ARRAY[1], ARRAY[NULL])");
        testUnloadColumnType(format, "map(varchar, int)", "NULL");
    }

    @ParameterizedTest
    @EnumSource(mode = Mode.EXCLUDE, names = {"CSV", "REGEX", "ESRI", "ESRI_GEO_JSON", "SEQUENCEFILE_PROTOBUF"})
    void testUnloadRow(HiveStorageFormat format)
            throws Exception
    {
        testUnloadColumnType(format, "row(f varchar)", "ROW('struct')");
        testUnloadColumnType(format, "row(f varchar)", "NULL");
    }

    private void testUnloadColumnType(HiveStorageFormat format, String columnType, String inputValue)
            throws Exception
    {
        testUnloadColumnType(getSession(), format, columnType, inputValue);
    }

    private void testUnloadColumnType(Session session, HiveStorageFormat format, String columnType, String inputValue)
            throws Exception
    {
        String tableName = "test_unload_format_" + randomNameSuffix();
        String location = directory.resolve(tableName).toUri().toString();
        Files.createDirectory(directory.resolve(tableName));

        MaterializedResult result = computeActual(session, "SELECT * FROM TABLE(hive.system.unload(" +
                "input => TABLE(SELECT CAST(" + inputValue + " AS " + columnType + ") x)," +
                "location => '" + location + "'," +
                "format => '" + format.name() + "'))");
        assertThat(result.getColumnNames()).containsExactly("path", "count");
        assertThat(result.getRowCount()).isEqualTo(1);

        assertUpdate(session, "CREATE TABLE " + tableName + "(x " + columnType + ") WITH (external_location = '" + location + "', format = '" + format.name() + "')");
        assertThat(query(session, "SELECT x FROM " + tableName))
                .matches("SELECT CAST(" + inputValue + " AS " + columnType + ")");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    void testUnloadEmptyResult()
            throws Exception
    {
        String tableName = "test_unload_empty_result_" + randomNameSuffix();
        String location = directory.resolve(tableName).toUri().toString();
        Files.createDirectory(directory.resolve(tableName));

        MaterializedResult result = computeActual("SELECT * FROM TABLE(hive.system.unload(" +
                "input => TABLE(SELECT CAST(name AS varchar) name FROM tpch.tiny.region WHERE false), " +
                "location => '" + location + "'," +
                "format => 'CSV'))");
        assertThat(result.getColumnNames()).containsExactly("path", "count");
        assertThat(result.getRowCount()).isEqualTo(0);
        assertThat(directory.resolve(tableName)).isEmptyDirectory();
    }

    @Test
    void testUnloadAvro()
            throws Exception
    {
        String tableName = "test_unload_avro_" + randomNameSuffix();
        String location = directory.resolve(tableName).toUri().toString();
        Files.createDirectory(directory.resolve(tableName));

        MaterializedResult result = computeActual("SELECT * FROM TABLE(hive.system.unload(" +
                "input => TABLE(SELECT CAST(127 AS tinyint), CAST(32767 AS smallint)) t(_tiny, _small)," +
                "location => '" + location + "'," +
                "format => 'AVRO'))");
        assertThat(result.getColumnNames()).containsExactly("path", "count");
        assertThat(result.getRowCount()).isEqualTo(1);

        // Create table with integer type as tinyint and smallint types aren't supported by Avro
        assertUpdate("CREATE TABLE " + tableName + "(_tiny integer, _small integer) WITH (external_location = '" + location + "', format = 'AVRO')");
        assertQuery("SELECT * FROM " + tableName, "VALUES (127, 32767)");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    void testUnloadCsvBoundedVarchar()
            throws Exception
    {
        String tableName = "test_unload_csv_varchar_" + randomNameSuffix();
        String location = directory.resolve(tableName).toUri().toString();
        Files.createDirectory(directory.resolve(tableName));

        MaterializedResult result = computeActual("SELECT * FROM TABLE(hive.system.unload(" +
                "input => TABLE(SELECT 'test data') t(col)," +
                "location => '" + location + "'," +
                "format => 'CSV'))");
        assertThat(result.getColumnNames()).containsExactly("path", "count");
        assertThat(result.getRowCount()).isEqualTo(1);

        assertUpdate("CREATE TABLE " + tableName + "(col varchar) WITH (external_location = '" + location + "', format = 'CSV')");
        assertQuery("SELECT * FROM " + tableName, "VALUES 'test data'");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    void testUnloadUnsupportedColumnType()
            throws Exception
    {
        String tableName = "test_unsupported_column_type_" + randomNameSuffix();
        String location = directory.resolve(tableName).toUri().toString();
        Files.createDirectory(directory.resolve(tableName));
        assertQueryFails(
                "SELECT * FROM TABLE(hive.system.unload(input => TABLE(SELECT JSON '{}' x), location => '" + location + "', format => 'ORC'))",
                "Unsupported Hive type: variant");
    }

    @Test
    void testUnloadCsvUnsupportedType()
    {
        String location = directory.resolve("test_csv_unsupported_type").toUri().toString();
        assertQueryFails(
                "SELECT * FROM TABLE(hive.system.unload(input => TABLE(SELECT 1 x), location => '" + location + "', format => 'CSV'))",
                "CSV only supports VARCHAR columns.*");
    }

    @Test
    void testUnloadRegexp()
    {
        String location = directory.resolve("test_regexp").toUri().toString();
        assertQueryFails(
                "SELECT * FROM TABLE(hive.system.unload(input => TABLE(SELECT 1 x), location => '" + location + "', format => 'REGEX'))",
                "REGEX format is read-only");
    }

    @Test
    void testUnloadAnonymousColumn()
    {
        String location = directory.resolve("test_anonymous_column").toUri().toString();
        assertQueryFails(
                "SELECT * FROM TABLE(hive.system.unload(input => TABLE(SELECT 1), location => '" + location + "', format => 'CSV'))",
                "Column name not specified at position 1");
        assertQueryFails(
                "SELECT * FROM TABLE(hive.system.unload(input => TABLE(SELECT 1 col, 2), location => '" + location + "', format => 'TEXTFILE'))",
                "Column name not specified at position 2");
        assertQueryFails(
                "SELECT * FROM TABLE(hive.system.unload(input => TABLE(SELECT 1, 2), location => '" + location + "', format => 'TEXTFILE'))",
                "Column name not specified at position 1");
    }

    @Test
    void testUnloadNotExistingDirectory()
    {
        String location = directory.resolve("test_not_existing_directory").toUri().toString();
        assertQueryFails(
                "SELECT * FROM TABLE(hive.system.unload(input => TABLE(SELECT 1 x), location => '" + location + "', format => 'ORC'))",
                "Location does not exist: " + location);
    }

    @Test
    void testUnloadIgnoreNotExistingDirectory()
            throws Exception
    {
        String location = directory.resolve("test_ignore_not_existing_directory").toUri().toString();

        MaterializedResult result = computeActual("SELECT * FROM TABLE(hive.system.unload(input => TABLE(SELECT 1 x), location => '" + location + "', format => 'TEXTFILE', existing_directory => 'ignore'))");

        assertThat(result.getRowCount()).isEqualTo(1);
        assertThat(Files.readString(Path.of(URI.create((String) result.getMaterializedRows().getFirst().getField(0)))))
                .isEqualTo(
                        """
                        1
                        """);

        // 'ignore' existing_directory should fail if the location is not empty
        assertQueryFails(
                "SELECT * FROM TABLE(hive.system.unload(input => TABLE(SELECT 1 x), location => '" + location + "', format => 'ORC', existing_directory => 'ignore'))",
                "Location must be empty: " + location);
    }

    @Test
    void testUnloadNonEmptyDirectory()
            throws Exception
    {
        String tableName = "test_unload_non_empty_result_" + randomNameSuffix();
        String location = directory.resolve(tableName).toUri().toString();
        Files.createDirectory(directory.resolve(tableName));
        Files.createFile(directory.resolve(tableName).resolve("test_file.txt"));

        assertQueryFails(
                "SELECT * FROM TABLE(hive.system.unload(input => TABLE(SELECT 1 x), location => '" + location + "', format => 'ORC'))",
                "Location must be empty: " + location);

        try (Stream<Path> files = Files.list(directory.resolve(tableName))) {
            assertThat(files)
                    .extracting(file -> file.getFileName().toString())
                    .containsExactly("test_file.txt");
        }
    }

    @Test
    void testUnloadUnsupportedRetryPolicy()
    {
        String location = directory.resolve("test_retry_policy").toUri().toString();

        Session taskRetrysession = Session.builder(getSession())
                .setSystemProperty("retry_policy", "TASK")
                .build();
        Session queryRetrysession = Session.builder(getSession())
                .setSystemProperty("retry_policy", "QUERY")
                .build();

        assertQueryFails(
                taskRetrysession,
                "SELECT * FROM TABLE(hive.system.unload(input => TABLE(SELECT 1 x), location => '" + location + "', format => 'ORC'))",
                "The function does not support query retries");
        assertQueryFails(
                queryRetrysession,
                "SELECT * FROM TABLE(hive.system.unload(input => TABLE(SELECT 1 x), location => '" + location + "', format => 'ORC'))",
                "The function does not support query retries");
    }

    @Test
    void testUnloadInvalidArgument()
    {
        String location = directory.resolve("test_missing_argument").toUri().toString();
        // missing argument
        assertQueryFails(
                "SELECT * FROM TABLE(hive.system.unload(location => '" + location + "', format => 'ORC'))",
                ".* Missing argument: INPUT");
        assertQueryFails(
                "SELECT * FROM TABLE(hive.system.unload(input => TABLE(SELECT 1 x), format => 'ORC'))",
                ".* Missing argument: LOCATION");
        assertQueryFails(
                "SELECT * FROM TABLE(hive.system.unload(input => TABLE(SELECT 1 x), location => '" + location + "'))",
                ".* Missing argument: FORMAT");

        // null argument
        assertQueryFails(
                "SELECT * FROM TABLE(hive.system.unload(input => NULL, location => '" + location + "', format => 'ORC'))",
                ".* Invalid argument INPUT. Expected table, got expression");
        assertQueryFails(
                "SELECT * FROM TABLE(hive.system.unload(input => TABLE(SELECT 1 x), location => NULL, format => 'ORC'))",
                "location cannot be null");
        assertQueryFails(
                "SELECT * FROM TABLE(hive.system.unload(input => TABLE(SELECT 1 x), location => '" + location + "', format => NULL))",
                "format cannot be null");
        assertQueryFails(
                "SELECT * FROM TABLE(hive.system.unload(input => TABLE(SELECT 1 x), location => '" + location + "', format => 'ORC', compression => null))",
                "compression cannot be null");
        assertQueryFails(
                "SELECT * FROM TABLE(hive.system.unload(input => TABLE(SELECT 1 x), location => '" + location + "', format => 'ORC', existing_directory => null))",
                "existing_directory cannot be null");

        // invalid format
        assertQueryFails(
                "SELECT * FROM TABLE(hive.system.unload(input => TABLE(SELECT 1 x), location => '" + location + "', format => 'WRONG'))",
                "WRONG format isn't supported");

        // invalid existing_directory
        assertQueryFails(
                "SELECT * FROM TABLE(hive.system.unload(input => TABLE(SELECT 1 x), location => '" + location + "', format => 'ORC', existing_directory => 'WRONG'))",
                "WRONG existing_directory isn't supported");
    }

    @Test
    void testUnloadInvalidPartitionArgument()
    {
        String location = directory.resolve("test_missing_argument").toUri().toString();

        // input contains only partition columns
        assertQueryFails(
                "SELECT * FROM TABLE(hive.system.unload(input => TABLE(SELECT 1 x) PARTITION BY x, location => '" + location + "', format => 'ORC'))",
                "INPUT contains only partition columns");
        assertQueryFails(
                "SELECT * FROM TABLE(hive.system.unload(input => TABLE(SELECT 1 x, 2 y) PARTITION BY (x, y), location => '" + location + "', format => 'ORC'))",
                "INPUT contains only partition columns");
    }

    @ParameterizedTest
    @EnumSource(mode = Mode.EXCLUDE, names = {"TEXTFILE", "CSV", "REGEX"})
    void testUnloadInvalidSeparatorArgument(HiveStorageFormat format)
    {
        String location = directory.resolve("test_missing_argument").toUri().toString();
        assertQueryFails(
                "SELECT * FROM TABLE(hive.system.unload(input => TABLE(SELECT 1 x), location => '" + location + "', format => '" + format + "', separator => '\t'))",
                "Cannot specify separator for storage format: " + format);
    }

    @ParameterizedTest
    @EnumSource(mode = Mode.EXCLUDE, names = {"TEXTFILE", "CSV", "REGEX"})
    void testUnloadInvalidHeaderArgument(HiveStorageFormat format)
    {
        String location = directory.resolve("test_invalid_header_argument").toUri().toString();
        assertQueryFails(
                "SELECT * FROM TABLE(hive.system.unload(input => TABLE(SELECT 1 x), location => '" + location + "', format => '" + format + "', header => true))",
                "Cannot specify header for storage format: " + format);
        assertQueryFails(
                "SELECT * FROM TABLE(hive.system.unload(input => TABLE(SELECT 1 x), location => '" + location + "', format => '" + format + "', header => false))",
                "Cannot specify header for storage format: " + format);
    }

    @ParameterizedTest
    @EnumSource(mode = Mode.EXCLUDE, names = { "REGEX", "ESRI", "ESRI_GEO_JSON", "SEQUENCEFILE_PROTOBUF" })
    void testUnloadWithSortOrder(HiveStorageFormat format)
            throws Exception
    {
        testUnloadWithSortOrder(false, format);
        testUnloadWithSortOrder(true, format);
    }

    private void testUnloadWithSortOrder(boolean partitioned, HiveStorageFormat format)
            throws Exception
    {
        String tableName = "test_unload_with_sort_" + randomNameSuffix();
        String location = directory.resolve(tableName).toUri().toString();
        Files.createDirectory(directory.resolve(tableName));

        String partition = partitioned ? "PARTITION BY part " : "";
        MaterializedResult result = computeActual(
                """
                SELECT * FROM
                TABLE (
                    hive.system.unload(
                        input => TABLE(
                        VALUES ('1', 'val1', 'part1'), ('5', 'val2', 'part1'), ('4', 'val3', 'part2'), ('3', 'val4', 'part2'), ('2', 'val5', 'part3')
                        ) t(sort_key, val, part) %s ORDER BY sort_key,
                        location => '%s',
                        format => '%s'))
                """.formatted(partition, location, format.name()));
        List<String> expectedColumnNames = partitioned ? ImmutableList.of("path", "count", "part") : ImmutableList.of("path", "count");
        int rowCount = partitioned ? 3 : 1;
        assertThat(result.getColumnNames()).containsExactly(expectedColumnNames.toArray(new String[0]));
        assertThat(result.getRowCount()).isEqualTo(rowCount);

        assertUpdate("CREATE TABLE " + tableName + "(sort_key varchar, val varchar, part varchar) " +
                "WITH (" + (partitioned ? "partitioned_by=ARRAY['part']," : "") +
                "external_location = '" + location + "', " +
                "format = '" + format.name() + "')");

        // using `containsExactly` to indicate the sortorder
        assertThat(computeActual("SELECT * FROM " + tableName).getMaterializedRows())
                .containsExactly(computeActual("SELECT * FROM " + tableName + " ORDER BY sort_key").getMaterializedRows().toArray(new MaterializedRow[0]));

        assertUpdate("DROP TABLE " + tableName);
    }

    @ParameterizedTest
    @MethodSource("testUnloadCompressionSource")
    void testUnloadOutputFileExtension(HiveStorageFormat format, HiveCompressionOption compression)
            throws IOException
    {
        String tableName = "test_unload_output_file_extension_" + randomNameSuffix();
        String location = directory.resolve(tableName).toUri().toString();
        Files.createDirectory(directory.resolve(tableName));

        String unload = "SELECT * FROM TABLE(hive.system.unload(" +
                "input => TABLE(SELECT '1' x), " +
                "location => '" + location + "'," +
                "format => '" + format.name() + "'," +
                "compression => '" + compression.name() + "'))";

        if ((format == PARQUET) && compression == HiveCompressionOption.LZ4) {
            assertThat(query(unload))
                    .nonTrinoExceptionFailure().hasMessageMatching("Unsupported compression codec for Parquet: LZ4");
            abort();
        }

        MaterializedResult result = computeActual(unload);
        assertThat(result.getColumnNames()).containsExactly("path", "count");
        assertThat(result.getRowCount()).isEqualTo(1);

        String fileExtension = UnloadWriterFactory.getFileExtension(toCompressionCodec(compression), format);
        assertThat((String) result.getMaterializedRows().getFirst().getField(0)).endsWith(fileExtension);
    }
}
