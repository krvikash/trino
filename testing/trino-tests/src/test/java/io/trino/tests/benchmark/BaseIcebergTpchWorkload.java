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
import io.trino.tpch.TpchTable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.io.Resources.getResource;
import static io.trino.tests.benchmark.BenchmarkRunner.applyDataGenerationConfiguration;
import static io.trino.tests.benchmark.IcebergTablesUtil.findTableDirectory;
import static io.trino.tests.benchmark.IcebergTablesUtil.resolveTablesLocation;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Shared Iceberg TPC-H workload definition. Subclasses bind a specific scale factor.
 */
public abstract class BaseIcebergTpchWorkload
        implements Workload
{
    private static final Logger log = Logger.get(BaseIcebergTpchWorkload.class);

    private static final List<String> TABLES = TpchTable.getTables().stream()
            .map(TpchTable::getTableName)
            .collect(toImmutableList());

    protected final int scaleFactor;

    protected BaseIcebergTpchWorkload(int scaleFactor)
    {
        this.scaleFactor = scaleFactor;
    }

    @Override
    public String name()
    {
        return "iceberg-tpch-sf%d".formatted(scaleFactor);
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
                    .replace("${database}", "iceberg")
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
    public DistributedQueryRunner createRunner(String dataLocation, BenchmarkRunner.ExecutionMode mode, boolean bind8080, Optional<Path> rmmLogPath, Optional<Path> fsCacheDirectory)
            throws Exception
    {
        IcebergQueryRunner.Builder builder = IcebergQueryRunner.builder()
                .setMetastoreDirectory(Path.of(dataLocation).toFile())
                .setWorkerCount(0)
                .disableSchemaInitializer()
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

        runner.execute("CREATE SCHEMA IF NOT EXISTS iceberg.tpch");
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
                .addIcebergProperty("iceberg.compression-codec", "SNAPPY")
                .addIcebergProperty("parquet.writer.page-value-count", "100000")
                .build()) {
            // IcebergQueryRunner.Builder creates a default "tpch" catalog with DOUBLE mapping.
            // Add a second one with DECIMAL so LIKE picks up the correct column types.
            runner.createCatalog("tpch_decimal", "tpch", Map.of("tpch.double-type-mapping", "DECIMAL"));
            Session session = BenchmarkRunner.withSingleWriter(runner.getDefaultSession());
            String schemaLocation = target.toAbsolutePath().normalize()
                    .relativize(tablesLocation.toAbsolutePath().normalize())
                    .toString();
            runner.execute(session, "CREATE SCHEMA iceberg.tpch WITH (location = 'local:///%s')".formatted(schemaLocation));
            for (String table : TABLES) {
                log.info("Generating iceberg.sf%d.%s", scaleFactor, table);
                runner.execute(session, "CREATE TABLE iceberg.tpch.%s WITH (format = 'PARQUET') AS SELECT * FROM tpch_decimal.sf%d.%s"
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
            log.info("Verifying row count: iceberg.tpch.%s vs tpch.sf%d.%s", table, scaleFactor, table);
            assertThat(assertions.query("SELECT count(*) FROM iceberg.tpch." + table))
                    .matches("SELECT count(*) FROM tpch.sf%d.%s".formatted(scaleFactor, table));
        }
    }

    @Override
    public List<String> tablesForStats()
    {
        return TABLES.stream().map(table -> "iceberg.tpch." + table).toList();
    }

    @Override
    public String expectedResultResource(int queryNumber)
    {
        return "sql/trino/tpch/sf%d/results/q%02d.ndjson".formatted(scaleFactor, queryNumber);
    }

    private static void registerTable(DistributedQueryRunner runner, String dataLocation, String table)
    {
        long tableCount = (Long) runner.execute(
                        "SELECT count(*) FROM iceberg.information_schema.tables WHERE table_schema = 'tpch' AND table_name = '%s'".formatted(table))
                .getOnlyValue();
        if (tableCount > 0) {
            log.info("Reusing existing iceberg.tpch.%s", table);
            return;
        }
        Path tableDir = findTableDirectory(resolveTablesLocation(dataLocation), table);
        String relativePath = Path.of(dataLocation).toAbsolutePath().normalize()
                .relativize(tableDir.toAbsolutePath().normalize())
                .toString();
        String location = "local:///" + relativePath;
        log.info("Registering iceberg.tpch.%s at %s", table, location);
        runner.execute("CALL iceberg.system.register_table('tpch', '%s', '%s')".formatted(table, location));
    }
}
