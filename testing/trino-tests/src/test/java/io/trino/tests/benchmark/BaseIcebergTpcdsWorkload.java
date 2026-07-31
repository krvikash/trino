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

import io.airlift.log.Logger;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.HiveMetastoreFactory;
import io.trino.plugin.iceberg.IcebergConnector;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.sql.query.QueryAssertions;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.containers.Minio;
import io.trino.tpcds.Table;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.testing.containers.Minio.MINIO_REGION;
import static io.trino.testing.containers.Minio.MINIO_ROOT_PASSWORD;
import static io.trino.testing.containers.Minio.MINIO_ROOT_USER;
import static io.trino.tests.benchmark.BenchmarkRunner.isRemote;
import static io.trino.tests.benchmark.IcebergTablesUtil.registerTables;
import static java.util.Locale.ENGLISH;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Shared Iceberg TPC-DS workload definition. Subclasses bind a specific scale factor.
 */
public abstract class BaseIcebergTpcdsWorkload
        implements Workload
{
    private static final Logger log = Logger.get(BaseIcebergTpcdsWorkload.class);

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
    public List<String> defaultQueries()
    {
        return Tpcds.allQueries();
    }

    @Override
    public String normalizeQuery(String query)
    {
        return Tpcds.normalizeQuery(query);
    }

    @Override
    public String readQuery(String query)
    {
        return Tpcds.readQuery(query, "iceberg", "tpcds");
    }

    @Override
    public String defaultDataLocation()
    {
        return Path.of(System.getProperty("user.home"), "starburst-benchmark-data", name()).toString();
    }

    @Override
    public DistributedQueryRunner createRunner(String dataLocation, BenchmarkRunner.ExecutionMode mode, boolean bind8080, Optional<Path> fsCacheDirectory)
            throws Exception
    {
        if (isRemote(dataLocation)) {
            throw new UnsupportedOperationException("Remote data locations are not supported for Iceberg benchmarks. Use a local path.");
        }
        Path minioDataDir = Path.of(dataLocation, "minio-data");
        Files.createDirectories(minioDataDir);

        Minio minio = Minio.builder().build();
        minio.mountDataDirectory(minioDataDir.toString());
        minio.start();

        IcebergQueryRunner.Builder builder = IcebergQueryRunner.builder()
                .setMetastoreDirectory(Path.of(dataLocation).toFile())
                .setWorkerCount(0)
                .disableSchemaInitializer()
                .setTpcdsCatalogEnabled(true)
                .addIcebergProperty("iceberg.register-table-procedure.enabled", "true")
                .addIcebergProperty("fs.s3.enabled", "true")
                .addIcebergProperty("s3.aws-access-key", MINIO_ROOT_USER)
                .addIcebergProperty("s3.aws-secret-key", MINIO_ROOT_PASSWORD)
                .addIcebergProperty("s3.region", MINIO_REGION)
                .addIcebergProperty("s3.endpoint", minio.getMinioAddress())
                .addIcebergProperty("s3.path-style-access", "true")
                .registerResource(minio);
        if (bind8080) {
            builder.addCoordinatorProperty("http-server.http.port", "8080");
        }
        BenchmarkRunner.applyExecutionMode(builder, mode);

        Map<String, String> fsCacheProperties = new HashMap<>();
        BenchmarkRunner.applyFilesystemCache(builder, fsCacheProperties, fsCacheDirectory);
        fsCacheProperties.forEach(builder::addIcebergProperty);

        if (mode == BenchmarkRunner.ExecutionMode.GPU) {
            builder.addIcebergProperty("iceberg.max-split-size", "512MB")
                    .addIcebergProperty("iceberg.experimental.composite-splits.enabled", "true");
        }
        DistributedQueryRunner runner = builder.build();
        runner.execute("CREATE SCHEMA IF NOT EXISTS iceberg.tpcds");

        HiveMetastore hiveMetastore = ((IcebergConnector) runner.getCoordinator().getConnector("iceberg")).getInjector()
                .getInstance(HiveMetastoreFactory.class)
                .createMetastore(Optional.empty());
        registerTables(minio, hiveMetastore, "starburst-benchmarks-data", dataLocation, "tpcds", TABLES, "iceberg-tpcds-sf%d-parquet".formatted(scaleFactor));

        return runner;
    }

    @Override
    public void generateData(Path target)
    {
        throw new UnsupportedOperationException("Iceberg local benchmarks use externally generated datasets.");
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
    public String expectedResultResource(String query)
    {
        // For Iceberg, `CHAR(N)` columns are stored as `VARCHAR` (Iceberg has no CHAR type).
        // Trailing spaces are stripped during data generation via `TRIM(CAST(col AS VARCHAR(N)))`.
        // This changes the expected string values, so `results_varchar` must be used instead of `results`.
        return "sql/trino/tpcds/sf%d/results_varchar/%s.ndjson".formatted(scaleFactor, query);
    }
}
