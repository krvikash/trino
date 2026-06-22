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
import io.trino.plugin.hive.TestingHivePlugin;
import io.trino.plugin.tpch.DecimalTypeMapping;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.sql.query.QueryAssertions;
import io.trino.testing.DistributedQueryRunner;
import io.trino.tpch.TpchTable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.io.Resources.getResource;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.tests.GpuQueriesTests.getTpchQueries;
import static io.trino.tests.benchmark.BenchmarkRunner.applyDataGenerationConfiguration;
import static io.trino.tests.benchmark.BenchmarkRunner.isRemote;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Shared TPC-H workload definition. Subclasses bind a specific scale factor.
 */
public abstract class BaseHiveTpchWorkload
        implements Workload
{
    private static final Logger log = Logger.get(BaseHiveTpchWorkload.class);

    private static final List<String> TABLES = TpchTable.getTables().stream()
            .map(TpchTable::getTableName)
            .collect(toImmutableList());

    private static final Pattern EXTERNAL_LOCATION_PATTERN = Pattern.compile("external_location\\s*=\\s*'([^']+)'");

    protected final int scaleFactor;

    protected BaseHiveTpchWorkload(int scaleFactor)
    {
        this.scaleFactor = scaleFactor;
    }

    @Override
    public String name()
    {
        return "tpch-sf%d".formatted(scaleFactor);
    }

    @Override
    public List<String> defaultQueries()
    {
        return getTpchQueries().toList();
    }

    @Override
    public String readQuery(String query)
    {
        try {
            return Resources.toString(
                            getResource("sql/trino/tpch/%s.sql".formatted(query)), UTF_8)
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
    public String defaultDataLocation()
    {
        return Path.of(System.getProperty("user.home"), "starburst-benchmark-data", name()).toString();
    }

    @Override
    public void validateDataLocation(String dataLocation)
    {
        Path path = Path.of(dataLocation);
        if (!Files.isDirectory(path)) {
            throw new IllegalStateException("Data location " + dataLocation
                    + " does not exist. Run the workload's Generator entry point to create it.");
        }
        for (String table : TABLES) {
            Path tableDir = path.resolve(table);
            if (!Files.isDirectory(tableDir)) {
                throw new IllegalStateException("Table directory " + tableDir
                        + " does not exist. Run the workload's Generator entry point to recreate the dataset.");
            }
        }
    }

    @Override
    public DistributedQueryRunner createRunner(String dataLocation, BenchmarkRunner.ExecutionMode mode, boolean bind8080, Optional<Path> fsCacheDirectory)
            throws Exception
    {
        // Persist the in-process FileHiveMetastore (and therefore ANALYZE-collected stats)
        // across runs so the harness doesn't re-ANALYZE every invocation.
        // Wipe the directory if the underlying parquet files change.
        Path metastoreDir = Path.of(System.getProperty("user.home"), "starburst-benchmark-data", ".metastore", name());
        Files.createDirectories(metastoreDir);
        DistributedQueryRunner.Builder<?> builder = DistributedQueryRunner.builder(
                testSessionBuilder()
                        .setCatalog("hive")
                        .setSchema("tpch")
                        .build());

        builder.setWorkerCount(0);
        builder.setBaseDataDir(Optional.of(metastoreDir));
        if (bind8080) {
            builder.addCoordinatorProperty("http-server.http.port", "8080");
        }
        BenchmarkRunner.applyExecutionMode(builder, mode);

        DistributedQueryRunner runner = builder.build();

        // tpch source catalog supplies column types for the CREATE TABLE ... LIKE below.
        runner.installPlugin(new TpchPlugin());
        runner.createCatalog("tpch", "tpch", Map.of("tpch.double-type-mapping", DecimalTypeMapping.DECIMAL.name()));

        Map<String, String> hiveProperties = new HashMap<>();
        hiveProperties.put("hive.parquet.time-zone", "UTC");
        if (isRemote(dataLocation)) {
            hiveProperties.put("fs.s3.enabled", "true");
            BenchmarkRunner.applyFilesystemCache(hiveProperties, fsCacheDirectory);
        }
        else {
            hiveProperties.put("hive.metastore.catalog.dir", "local://" + metastoreDir.resolve("hive").toAbsolutePath());
        }
        if (mode == BenchmarkRunner.ExecutionMode.GPU) {
            hiveProperties.put("hive.max-split-size", "512MB");
            hiveProperties.put("hive.parquet.max-split-size", "512MB");
        }

        // local:// routes reads through the native LocalFileSystem; root "/" so absolute data paths resolve.
        Path localFileSystemRoot;
        if (isRemote(dataLocation)) {
            localFileSystemRoot = runner.getCoordinator().getBaseDataDir().resolve("hive_data");
        }
        else {
            localFileSystemRoot = Path.of("/");
        }
        runner.installPlugin(new TestingHivePlugin(localFileSystemRoot));
        runner.createCatalog("hive", "hive", hiveProperties);

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
        try (DistributedQueryRunner runner = applyDataGenerationConfiguration(HiveQueryRunner.builder())
                .setSkipTimezoneSetup(true)
                .setCreateTpchSchemas(false)
                .setTpchDecimalTypeMapping(DecimalTypeMapping.DECIMAL)
                .addHiveProperty("hive.parquet.time-zone", "UTC")
                .addHiveProperty("hive.metastore.disable-location-checks", "true")
                .addHiveProperty("hive.compression-codec", "SNAPPY")
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
    public String expectedResultResource(String query)
    {
        return "sql/trino/tpch/sf%d/results/%s.ndjson".formatted(scaleFactor, query);
    }

    private void createExternalTable(DistributedQueryRunner runner, String dataLocation, String table)
    {
        String location = isRemote(dataLocation)
                ? dataLocation + "/" + table
                : "local://" + Path.of(dataLocation, table).toAbsolutePath();
        Optional<String> existing = readExistingExternalLocation(runner, table);
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
        runner.execute(
                """
                CREATE TABLE hive.tpch.%s (LIKE tpch.sf%d.%s)
                WITH (external_location = '%s', format = 'PARQUET')
                """.formatted(table, scaleFactor, table, location));
    }

    private static Optional<String> readExistingExternalLocation(DistributedQueryRunner runner, String table)
    {
        if ((Long) runner.execute("SELECT count(*) FROM hive.information_schema.tables WHERE table_schema = 'tpch' AND table_name = '%s'".formatted(table)).getOnlyValue() == 0) {
            return Optional.empty();
        }
        String createSql = (String) runner.execute("SHOW CREATE TABLE hive.tpch." + table).getOnlyValue();
        Matcher matcher = EXTERNAL_LOCATION_PATTERN.matcher(createSql);
        if (!matcher.find()) {
            return Optional.empty();
        }
        return Optional.of(matcher.group(1));
    }
}
