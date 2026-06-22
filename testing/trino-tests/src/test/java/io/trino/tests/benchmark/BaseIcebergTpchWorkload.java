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
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.HiveMetastoreFactory;
import io.trino.plugin.iceberg.IcebergConnector;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.sql.query.QueryAssertions;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.containers.Minio;
import io.trino.tpch.TpchTable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.io.Resources.getResource;
import static io.trino.testing.containers.Minio.MINIO_REGION;
import static io.trino.testing.containers.Minio.MINIO_ROOT_PASSWORD;
import static io.trino.testing.containers.Minio.MINIO_ROOT_USER;
import static io.trino.tests.GpuQueriesTests.getTpchQueries;
import static io.trino.tests.benchmark.BenchmarkRunner.isRemote;
import static io.trino.tests.benchmark.IcebergTablesUtil.registerTables;
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
        BenchmarkRunner.applyFilesystemCache(fsCacheProperties, fsCacheDirectory);
        fsCacheProperties.forEach(builder::addIcebergProperty);

        if (mode == BenchmarkRunner.ExecutionMode.GPU) {
            builder.addIcebergProperty("iceberg.max-split-size", "512MB")
                    .addIcebergProperty("iceberg.experimental.composite-splits.enabled", "true");
        }
        DistributedQueryRunner runner = builder.build();
        runner.execute("CREATE SCHEMA IF NOT EXISTS iceberg.tpch");

        HiveMetastore hiveMetastore = ((IcebergConnector) runner.getCoordinator().getConnector("iceberg")).getInjector()
                .getInstance(HiveMetastoreFactory.class)
                .createMetastore(Optional.empty());
        registerTables(minio, hiveMetastore, "starburst-benchmarks-data", dataLocation, "tpch", TABLES, "iceberg-tpch-sf%d-parquet".formatted(scaleFactor));

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
    public String expectedResultResource(String query)
    {
        return "sql/trino/tpch/sf%d/results/%s.ndjson".formatted(scaleFactor, query);
    }
}
