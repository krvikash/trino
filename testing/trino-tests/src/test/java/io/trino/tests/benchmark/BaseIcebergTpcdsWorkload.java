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
package io.trino.tests.benchmark;

import com.google.common.io.Resources;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.sql.query.QueryAssertions;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.tpcds.Table;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.io.Resources.getResource;
import static io.trino.tests.benchmark.BenchmarkRunner.applyDataGenerationConfiguration;
import static io.trino.tests.benchmark.IcebergTableDirectoryFinder.findTableDirectory;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Locale.ENGLISH;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Shared Iceberg TPC-DS workload definition. Subclasses bind a specific scale factor.
 */
public abstract class BaseIcebergTpcdsWorkload
        implements Workload
{
    private static final Logger log = Logger.get(BaseIcebergTpcdsWorkload.class);

    private static final Pattern CHAR_TYPE_PATTERN = Pattern.compile("^char\\((\\d+)\\)$");

    private static final List<String> TABLES = Table.getBaseTables().stream()
            .filter(table -> table != Table.DBGEN_VERSION)
            .map(table -> table.getName().toLowerCase(ENGLISH))
            .collect(toImmutableList());

    protected final int scaleFactor;

    protected BaseIcebergTpcdsWorkload(int scaleFactor)
    {
        this.scaleFactor = scaleFactor;
    }

    @Override
    public String name()
    {
        return "iceberg-tpcds-sf%d".formatted(scaleFactor);
    }

    @Override
    public List<Integer> defaultQueries()
    {
        return IntStream.rangeClosed(1, 99).boxed().toList();
    }

    @Override
    public String readQuery(int queryNumber)
    {
        try {
            return Resources.toString(
                            getResource("sql/trino/tpcds/q%02d.sql".formatted(queryNumber)), UTF_8)
                    .replace("${database}", "iceberg")
                    .replace("${schema}", "tpcds")
                    .trim()
                    .replaceFirst(";$", "");
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public String defaultDataLocation()
    {
        return Path.of(System.getProperty("user.home"), "starburst-benchmark-data", name()).toString();
    }

    @Override
    public DistributedQueryRunner createRunner(String dataLocation, BenchmarkRunner.ExecutionMode mode, boolean bind8080, Optional<Path> rmmLogPath)
            throws Exception
    {
        IcebergQueryRunner.Builder builder = IcebergQueryRunner.builder()
                .setMetastoreDirectory(Path.of(dataLocation).toFile())
                .setWorkerCount(0)
                .disableSchemaInitializer()
                .setTpcdsCatalogEnabled(true)
                .addIcebergProperty("iceberg.register-table-procedure.enabled", "true");
        if (bind8080) {
            builder.addCoordinatorProperty("http-server.http.port", "8080");
        }
        rmmLogPath.ifPresent(path -> builder.setAdditionalModule(new RmmLoggingModule(path)));
        BenchmarkRunner.applyExecutionMode(builder, mode);
        if (mode == BenchmarkRunner.ExecutionMode.GPU) {
            builder.addIcebergProperty("iceberg.max-split-size", "512MB");
        }
        DistributedQueryRunner runner = builder.build();

        runner.execute("CREATE SCHEMA IF NOT EXISTS iceberg.tpcds");
        for (String table : TABLES) {
            registerTable(runner, dataLocation, table);
        }
        return runner;
    }

    @Override
    public void generateData(Path target)
            throws Exception
    {
        Path tablesLocation = resolveTablesLocation(target.toAbsolutePath().toString());
        try (DistributedQueryRunner runner = applyDataGenerationConfiguration(IcebergQueryRunner.builder())
                .disableSchemaInitializer()
                .setMetastoreDirectory(target.toFile())
                .setTpcdsCatalogEnabled(true)
                .addIcebergProperty("iceberg.compression-codec", "SNAPPY")
                .addIcebergProperty("parquet.writer.page-value-count", "100000")
                .build()) {
            Session session = BenchmarkRunner.withSingleWriter(runner.getDefaultSession());
            String schemaLocation = target.toAbsolutePath().normalize()
                    .relativize(tablesLocation.toAbsolutePath().normalize())
                    .toString();
            runner.execute(session, "CREATE SCHEMA iceberg.tpcds WITH (location = 'local:///%s')".formatted(schemaLocation));
            for (String table : TABLES) {
                String selectList = buildSelectList(runner, table);
                log.info("Generating iceberg.sf%d.%s", scaleFactor, table);
                runner.execute(session, format(
                        "CREATE TABLE iceberg.tpcds.%s WITH (format = 'PARQUET') AS SELECT %s FROM tpcds.sf%d.%s",
                        table,
                        selectList,
                        scaleFactor,
                        table));
            }
        }
        BenchmarkRunner.cleanCrcFiles(target);
    }

    private String buildSelectList(DistributedQueryRunner runner, String table)
    {
        MaterializedResult columns = runner.execute(format(
                "SELECT column_name, data_type FROM tpcds.information_schema.columns WHERE table_schema = 'sf%d' AND table_name = '%s' ORDER BY ordinal_position",
                scaleFactor,
                table));
        return columns.getMaterializedRows().stream()
                .map(row -> {
                    String columnName = (String) row.getField(0);
                    String dataType = (String) row.getField(1);
                    Matcher matcher = CHAR_TYPE_PATTERN.matcher(dataType);
                    if (matcher.matches()) {
                        // Iceberg has no CHAR type, so CHAR(N) columns become VARCHAR. CAST alone preserves
                        // trailing-space padding; TRIM strips it so that VARCHAR comparisons in benchmark
                        // queries (e.g. d_day_name = 'Sunday') match correctly.
                        return format("TRIM(CAST(\"%s\" AS VARCHAR(%s))) AS \"%s\"", columnName, matcher.group(1), columnName);
                    }
                    return format("\"%s\"", columnName);
                })
                .collect(joining(", "));
    }

    @Override
    public void verifyDataset(DistributedQueryRunner runner)
    {
        QueryAssertions assertions = new QueryAssertions(runner);
        for (String table : TABLES) {
            log.info("Verifying row count: iceberg.tpcds.%s vs tpcds.sf%d.%s", table, scaleFactor, table);
            assertThat(assertions.query("SELECT count(*) FROM iceberg.tpcds." + table))
                    .matches("SELECT count(*) FROM tpcds.sf%d.%s".formatted(scaleFactor, table));
        }
    }

    @Override
    public List<String> tablesForStats()
    {
        return TABLES.stream().map(table -> "iceberg.tpcds." + table).toList();
    }

    @Override
    public String expectedResultResource(int queryNumber)
    {
        // For Iceberg, `CHAR(N)` columns are stored as `VARCHAR` (Iceberg has no CHAR type).
        // Trailing spaces are stripped during data generation via `TRIM(CAST(col AS VARCHAR(N)))`.
        // This changes the expected string values, so `results_varchar` must be used instead of `results`.
        return "sql/trino/tpcds/sf%d/results_varchar/q%02d.ndjson".formatted(scaleFactor, queryNumber);
    }

    private static void registerTable(DistributedQueryRunner runner, String dataLocation, String table)
    {
        long tableCount = (Long) runner.execute(
                        "SELECT count(*) FROM iceberg.information_schema.tables WHERE table_schema = 'tpcds' AND table_name = '%s'".formatted(table))
                .getOnlyValue();
        if (tableCount > 0) {
            log.info("Reusing existing iceberg.tpcds.%s", table);
            return;
        }
        Path tableDir = findTableDirectory(resolveTablesLocation(dataLocation), table);
        String relativePath = Path.of(dataLocation).toAbsolutePath().normalize()
                .relativize(tableDir.toAbsolutePath().normalize())
                .toString();
        String location = "local:///" + relativePath;
        log.info("Registering iceberg.tpcds.%s at %s", table, location);
        runner.execute("CALL iceberg.system.register_table('tpcds', '%s', '%s')".formatted(table, location));
    }

    static Path resolveTablesLocation(String dataLocation)
    {
        return Path.of(dataLocation, "tables");
    }
}
