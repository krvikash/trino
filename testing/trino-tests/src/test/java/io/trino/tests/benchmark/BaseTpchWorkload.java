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
import io.trino.plugin.hive.HiveQueryRunner;
import io.trino.plugin.tpch.DecimalTypeMapping;
import io.trino.sql.query.QueryAssertions;
import io.trino.testing.DistributedQueryRunner;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

import static com.google.common.io.Resources.getResource;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Shared TPC-H workload definition. Subclasses bind a specific scale factor.
 */
public abstract class BaseTpchWorkload
        implements Workload
{
    private static final Logger log = Logger.get(BaseTpchWorkload.class);

    private static final List<String> TABLES = List.of(
            "region", "nation", "customer", "supplier", "part", "partsupp", "orders", "lineitem");

    private static final Pattern EXTERNAL_LOCATION_PATTERN = Pattern.compile("external_location\\s*=\\s*'([^']+)'");

    protected final int scaleFactor;

    protected BaseTpchWorkload(int scaleFactor)
    {
        this.scaleFactor = scaleFactor;
    }

    @Override
    public String name()
    {
        return "tpch-sf%d".formatted(scaleFactor);
    }

    @Override
    public List<Integer> defaultQueries()
    {
        return IntStream.rangeClosed(1, 22).boxed().toList();
    }

    @Override
    public String readQuery(int queryNumber)
    {
        try {
            return Resources.toString(
                            getResource("sql/trino/tpch/q%02d.sql".formatted(queryNumber)), UTF_8)
                    .replace("${database}", "hive")
                    .replace("${schema}", "tpch")
                    .replace("${prefix}", "")
                    .replace("${scale}", String.valueOf(scaleFactor))
                    .trim()
                    .replaceFirst(";$", "");
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public Path defaultDataLocation()
    {
        return Path.of(System.getProperty("user.home"), "starburst-benchmark-data", name());
    }

    @Override
    public void validateDataLocation(Path dataLocation)
    {
        if (!Files.isDirectory(dataLocation)) {
            throw new IllegalStateException("Data location " + dataLocation
                    + " does not exist. Run the workload's Generator entry point to create it.");
        }
        for (String table : TABLES) {
            Path tableDir = dataLocation.resolve(table);
            if (!Files.isDirectory(tableDir)) {
                throw new IllegalStateException("Table directory " + tableDir
                        + " does not exist. Run the workload's Generator entry point to recreate the dataset.");
            }
        }
    }

    @Override
    public DistributedQueryRunner createRunner(Path dataLocation, BenchmarkRunner.ExecutionMode mode)
            throws Exception
    {
        // Persist the in-process FileHiveMetastore (and therefore ANALYZE-collected stats)
        // across runs so the harness doesn't re-ANALYZE every invocation.
        // Wipe the directory if the underlying parquet files change.
        Path metastoreDir = Path.of(System.getProperty("user.home"), "starburst-benchmark-data", ".metastore", name());
        Files.createDirectories(metastoreDir);
        HiveQueryRunner.Builder<?> builder = HiveQueryRunner.builder()
                .setWorkerCount(0)
                .setBaseDataDir(Optional.of(metastoreDir))
                .addExtraProperty("query.max-memory-per-node", "80%")
                .addExtraProperty("query.max-memory", "1TB")
                .addExtraProperty("memory.heap-headroom-per-node", "20%")
                .setSkipTimezoneSetup(true)
                .addHiveProperty("hive.parquet.time-zone", "UTC")
                .setTpchDecimalTypeMapping(DecimalTypeMapping.DECIMAL);
        BenchmarkRunner.applyExecutionMode(builder, mode);
        DistributedQueryRunner runner = builder.build();

        runner.execute("CREATE SCHEMA IF NOT EXISTS hive.tpch");
        for (String table : TABLES) {
            createExternalTable(runner, dataLocation, table);
        }
        return runner;
    }

    @Override
    public void generateData(Path target)
            throws Exception
    {
        Files.createDirectories(target);
        try (DistributedQueryRunner runner = BenchmarkRunner.dataGenerationBuilder()
                .setCreateTpchSchemas(false)
                .setTpchDecimalTypeMapping(DecimalTypeMapping.DECIMAL)
                .build()) {
            Session session = BenchmarkRunner.withSingleWriter(runner.getDefaultSession());
            String schemaLocation = target.toAbsolutePath().normalize().toUri().toString();
            runner.execute(session, "CREATE SCHEMA hive.tpch WITH (location = '%s')".formatted(schemaLocation));
            for (String table : TABLES) {
                log.info("Generating tpch.sf%d.%s -> %s%s", scaleFactor, table, schemaLocation, table);
                runner.execute(session, "CREATE TABLE hive.tpch.%s WITH (format = 'PARQUET') AS SELECT * FROM tpch.sf%d.%s"
                        .formatted(table, scaleFactor, table));
            }
        }
        BenchmarkRunner.cleanCrcFiles(target);
    }

    @Override
    public void verifyDataset(DistributedQueryRunner runner)
    {
        QueryAssertions assertions = new QueryAssertions(runner);
        for (String table : TABLES) {
            log.info("Verifying row count: hive.tpch.%s vs tpch.sf%d.%s", table, scaleFactor, table);
            assertThat(assertions.query("SELECT count(*) FROM hive.tpch." + table))
                    .matches("SELECT count(*) FROM tpch.sf%d.%s".formatted(scaleFactor, table));
        }
    }

    @Override
    public List<String> tablesForStats()
    {
        return TABLES.stream().map(table -> "hive.tpch." + table).toList();
    }

    @Override
    public String expectedResultResource(int queryNumber)
    {
        return "sql/trino/tpch/sf%d/results/q%02d.ndjson".formatted(scaleFactor, queryNumber);
    }

    /**
     * LIKE (vs CTAS) keeps column types in sync with the tpch connector's
     * {@link DecimalTypeMapping} and skips {@code HiveLocationService.forNewTableAsSelect},
     * so the target directory may exist.
     *
     * <p>The Hive metastore is persisted across runs (see {@link #createRunner}), so a previously
     * created table at a different {@code external_location} would be silently reused by a plain
     * {@code CREATE TABLE IF NOT EXISTS}. Drop and recreate when the stored location no longer
     * matches the requested one.
     */
    private void createExternalTable(DistributedQueryRunner runner, Path dataLocation, String table)
    {
        Path location = dataLocation.resolve(table).toAbsolutePath().normalize();
        Optional<Path> existing = readExistingExternalLocation(runner, table);
        if (existing.isPresent() && existing.get().equals(location)) {
            log.info("Reusing existing hive.tpch.%s at %s", table, location);
            return;
        }
        if (existing.isPresent()) {
            log.info("Recreating hive.tpch.%s: was at %s, now at %s", table, existing.get(), location);
            runner.execute("DROP TABLE hive.tpch." + table);
        }
        else {
            log.info("Creating hive.tpch.%s at %s", table, location);
        }
        runner.execute("""
                CREATE TABLE hive.tpch.%s (LIKE tpch.sf%d.%s)
                WITH (external_location = '%s', format = 'PARQUET')
                """.formatted(table, scaleFactor, table, location));
    }

    private static Optional<Path> readExistingExternalLocation(DistributedQueryRunner runner, String table)
    {
        String createSql;
        try {
            createSql = (String) runner.execute("SHOW CREATE TABLE hive.tpch." + table).getOnlyValue();
        }
        catch (RuntimeException e) {
            return Optional.empty();
        }
        Matcher matcher = EXTERNAL_LOCATION_PATTERN.matcher(createSql);
        if (!matcher.find()) {
            return Optional.empty();
        }
        String stored = matcher.group(1);
        Path path = stored.startsWith("file:") ? Path.of(URI.create(stored)) : Path.of(stored);
        return Optional.of(path.toAbsolutePath().normalize());
    }
}
