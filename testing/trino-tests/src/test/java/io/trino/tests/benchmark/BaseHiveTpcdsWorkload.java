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
import io.trino.plugin.tpcds.TpcdsPlugin;
import io.trino.sql.query.QueryAssertions;
import io.trino.testing.DistributedQueryRunner;
import io.trino.tpcds.Table;

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
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.io.Resources.getResource;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.tests.benchmark.BenchmarkRunner.applyDataGenerationConfiguration;
import static io.trino.tests.benchmark.BenchmarkRunner.isRemote;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Locale.ENGLISH;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Shared TPC-DS workload definition. Subclasses bind a specific scale factor.
 */
public abstract class BaseHiveTpcdsWorkload
        implements Workload
{
    private static final Logger log = Logger.get(BaseHiveTpcdsWorkload.class);

    private static final List<String> TABLES = Table.getBaseTables().stream()
            .filter(table -> table != Table.DBGEN_VERSION)
            .map(table -> table.getName().toLowerCase(ENGLISH))
            .collect(toImmutableList());

    private static final Pattern EXTERNAL_LOCATION_PATTERN = Pattern.compile("external_location\\s*=\\s*'([^']+)'");

    protected final int scaleFactor;

    protected BaseHiveTpcdsWorkload(int scaleFactor)
    {
        this.scaleFactor = scaleFactor;
    }

    @Override
    public String name()
    {
        return "tpcds-sf%d".formatted(scaleFactor);
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
                    .replace("${database}", "hive")
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
    public DistributedQueryRunner createRunner(String dataLocation, BenchmarkRunner.ExecutionMode mode, boolean bind8080, Optional<Path> rmmLogPath, Optional<Path> fsCacheDirectory)
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
                        .setSchema("tpcds")
                        .build());

        builder.setWorkerCount(0);
        builder.setBaseDataDir(Optional.of(metastoreDir));
        if (bind8080) {
            builder.addCoordinatorProperty("http-server.http.port", "8080");
        }
        BenchmarkRunner.applyExecutionMode(builder, mode);
        rmmLogPath.ifPresent(path -> builder.setAdditionalModule(new RmmLoggingModule(path)));

        DistributedQueryRunner runner = builder.build();

        // tpcds source catalog supplies column types for the CREATE TABLE ... LIKE below.
        runner.installPlugin(new TpcdsPlugin());
        runner.createCatalog("tpcds", "tpcds");

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

        runner.execute("CREATE SCHEMA IF NOT EXISTS hive.tpcds");
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
                .setTpcdsCatalogEnabled(true)
                .addHiveProperty("hive.parquet.time-zone", "UTC")
                .addHiveProperty("hive.metastore.disable-location-checks", "true")
                .addHiveProperty("hive.compression-codec", "SNAPPY")
                .build()) {
            Session session = BenchmarkRunner.withSingleWriter(runner.getDefaultSession());
            String schemaLocation = target.toAbsolutePath().normalize().toUri().toString();
            runner.execute(session, "CREATE SCHEMA hive.tpcds WITH (location = '%s')".formatted(schemaLocation));
            for (String table : TABLES) {
                log.info("Generating tpcds.sf%d.%s -> %s%s", scaleFactor, table, schemaLocation, table);
                runner.execute(session, "CREATE TABLE hive.tpcds.%s WITH (format = 'PARQUET') AS SELECT * FROM tpcds.sf%d.%s"
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
            log.info("Verifying row count: hive.tpcds.%s vs tpcds.sf%d.%s", table, scaleFactor, table);
            assertThat(assertions.query("SELECT count(*) FROM hive.tpcds." + table))
                    .matches("SELECT count(*) FROM tpcds.sf%d.%s".formatted(scaleFactor, table));
        }
    }

    @Override
    public List<String> tablesForStats()
    {
        return TABLES.stream().map(table -> "hive.tpcds." + table).toList();
    }

    @Override
    public String expectedResultResource(int queryNumber)
    {
        return "sql/trino/tpcds/sf%d/results/q%02d.ndjson".formatted(scaleFactor, queryNumber);
    }

    private void createExternalTable(DistributedQueryRunner runner, String dataLocation, String table)
    {
        String location = isRemote(dataLocation)
                ? dataLocation + "/" + table
                : "local://" + Path.of(dataLocation, table).toAbsolutePath();
        Optional<String> existing = readExistingExternalLocation(runner, table);
        if (existing.isPresent() && existing.get().equals(location)) {
            log.info("Reusing existing hive.tpcds.%s at %s", table, location);
            return;
        }
        if (existing.isPresent()) {
            log.info("Recreating hive.tpcds.%s: was at %s, now at %s", table, existing.get(), location);
            runner.execute("DROP TABLE hive.tpcds." + table);
        }
        else {
            log.info("Creating hive.tpcds.%s at %s", table, location);
        }
        runner.execute(
                """
                CREATE TABLE hive.tpcds.%s (LIKE tpcds.sf%d.%s)
                WITH (external_location = '%s', format = 'PARQUET')
                """.formatted(table, scaleFactor, table, location));
    }

    private static Optional<String> readExistingExternalLocation(DistributedQueryRunner runner, String table)
    {
        if ((Long) runner.execute("SELECT count(*) FROM hive.information_schema.tables WHERE table_schema = 'tpcds' AND table_name = '%s'".formatted(table)).getOnlyValue() == 0) {
            return Optional.empty();
        }
        String createSql = (String) runner.execute("SHOW CREATE TABLE hive.tpcds." + table).getOnlyValue();
        Matcher matcher = EXTERNAL_LOCATION_PATTERN.matcher(createSql);
        if (!matcher.find()) {
            return Optional.empty();
        }
        return Optional.of(matcher.group(1));
    }
}
