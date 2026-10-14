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
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.HiveMetastoreFactory;
import io.trino.plugin.iceberg.IcebergConnector;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.sql.query.QueryAssertions;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.TestingAccessControlManager.TestingPrivilege;
import io.trino.testing.containers.Minio;
import io.trino.tpcds.Table;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.INSERT_TABLE;
import static io.trino.testing.containers.Minio.MINIO_REGION;
import static io.trino.testing.containers.Minio.MINIO_ROOT_PASSWORD;
import static io.trino.testing.containers.Minio.MINIO_ROOT_USER;
import static io.trino.tests.benchmark.BenchmarkRunner.isRemote;
import static io.trino.tests.benchmark.IcebergTablesUtil.registerTables;
import static io.trino.tests.benchmark.WarpIndexSupport.WARP_CATALOG;
import static io.trino.tests.benchmark.WarpIndexSupport.createWarpCatalog;
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

    protected String benchmarkCatalog = "iceberg";

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
        return Tpcds.readQuery(query, benchmarkCatalog, "tpcds");
    }

    @Override
    public String defaultDataLocation()
    {
        return Path.of(System.getProperty("user.home"), "starburst-benchmark-data", name()).toString();
    }

    @Override
    public DistributedQueryRunner createRunner(
            String dataLocation,
            BenchmarkRunner.ExecutionMode mode,
            boolean bind8080,
            Map<String, String> serverProperties,
            Optional<Path> fsCacheDirectory,
            Optional<Path> warpIndexDirectory)
            throws Exception
    {
        if (isRemote(dataLocation)) {
            return createRunnerForRemoteData(dataLocation, mode, bind8080, serverProperties, fsCacheDirectory, warpIndexDirectory);
        }
        Path minioDataDir = Path.of(dataLocation, "minio-data");
        Files.createDirectories(minioDataDir);

        Minio minio = Minio.builder().build();
        minio.mountDataDirectory(minioDataDir.toString());
        minio.start();

        Map<String, String> icebergProperties = new HashMap<>();
        icebergProperties.put("s3.aws-access-key", MINIO_ROOT_USER);
        icebergProperties.put("s3.aws-secret-key", MINIO_ROOT_PASSWORD);
        icebergProperties.put("s3.region", MINIO_REGION);
        icebergProperties.put("s3.endpoint", minio.getMinioAddress());
        icebergProperties.put("s3.path-style-access", "true");

        IcebergQueryRunner.Builder builder = IcebergQueryRunner.builder()
                .setMetastoreDirectory(Path.of(dataLocation).toFile())
                .registerResource(minio);
        DistributedQueryRunner runner = buildRunner(builder, icebergProperties, mode, bind8080, serverProperties, fsCacheDirectory);

        HiveMetastore hiveMetastore = ((IcebergConnector) runner.getCoordinator().getConnector("iceberg")).getInjector()
                .getInstance(HiveMetastoreFactory.class)
                .createMetastore(Optional.empty());
        registerTables(minio, hiveMetastore, "starburst-benchmarks-data", dataLocation, "tpcds", TABLES, "iceberg-tpcds-sf%d-parquet".formatted(scaleFactor));

        if (warpIndexDirectory.isPresent()) {
            addWarpCatalog(runner, icebergProperties, Path.of(dataLocation), warpIndexDirectory.get());
        }

        return runner;
    }

    // Reads the tables in place from S3 with the default AWS credential chain.
    private DistributedQueryRunner createRunnerForRemoteData(String dataLocation, BenchmarkRunner.ExecutionMode mode, boolean bind8080, Map<String, String> serverProperties, Optional<Path> fsCacheDirectory, Optional<Path> warpIndexDirectory)
            throws Exception
    {
        Map<String, String> icebergProperties = new HashMap<>();
        Path metastoreDirectory = Files.createTempDirectory("iceberg-tpcds-metastore");
        IcebergQueryRunner.Builder builder = IcebergQueryRunner.builder()
                .setMetastoreDirectory(metastoreDirectory.toFile())
                .registerResource(() -> deleteRecursively(metastoreDirectory, ALLOW_INSECURE));
        DistributedQueryRunner runner = buildRunner(builder, icebergProperties, mode, bind8080, serverProperties, fsCacheDirectory);

        TrinoFileSystem fileSystem = ((IcebergConnector) runner.getCoordinator().getConnector("iceberg")).getInjector()
                .getInstance(TrinoFileSystemFactory.class)
                .create(ConnectorIdentity.ofUser("benchmark"));
        List<String> tableLocations = fileSystem.listDirectories(Location.of(dataLocation)).stream()
                .map(location -> location.toString().replaceFirst("/$", ""))
                .collect(toImmutableList());
        for (String table : TABLES) {
            List<String> matches = tableLocations.stream()
                    .filter(location -> location.substring(location.lastIndexOf('/') + 1).matches(table + "-[0-9a-f]{32}"))
                    .collect(toImmutableList());
            if (matches.size() != 1) {
                throw new IllegalStateException("Expected one directory for table '%s' in %s, found %s".formatted(table, dataLocation, matches));
            }
            log.info("Registering table %s at %s", table, matches.getFirst());
            runner.execute("CALL iceberg.system.register_table(schema_name => 'tpcds', table_name => '%s', table_location => '%s')".formatted(table, matches.getFirst()));
        }

        if (warpIndexDirectory.isPresent()) {
            addWarpCatalog(runner, icebergProperties, metastoreDirectory, warpIndexDirectory.get());
        }

        // Denying INSERT also denies ANALYZE, so runs never write statistics to the S3 dataset.
        runner.getAccessControl().deny(new TestingPrivilege(Optional.empty(), _ -> true, INSERT_TABLE));
        return runner;
    }

    // Adds the properties shared by local and remote data to icebergProperties and builds the runner.
    private static DistributedQueryRunner buildRunner(
            IcebergQueryRunner.Builder builder,
            Map<String, String> icebergProperties,
            BenchmarkRunner.ExecutionMode mode,
            boolean bind8080,
            Map<String, String> serverProperties,
            Optional<Path> fsCacheDirectory)
            throws Exception
    {
        icebergProperties.put("iceberg.register-table-procedure.enabled", "true");
        icebergProperties.put("fs.s3.enabled", "true");
        builder.setWorkerCount(0)
                .disableSchemaInitializer()
                .setTpcdsCatalogEnabled(true);
        BenchmarkRunner.configureQueryRunner(builder, mode, bind8080, serverProperties);
        BenchmarkRunner.applyFilesystemCache(builder, icebergProperties, fsCacheDirectory);

        if (mode == BenchmarkRunner.ExecutionMode.GPU) {
            icebergProperties.put("iceberg.max-split-size", "512MB");
            icebergProperties.put("iceberg.experimental.composite-splits.enabled", "true");
        }
        DistributedQueryRunner runner = builder.addIcebergProperties(icebergProperties).build();
        runner.execute("CREATE SCHEMA IF NOT EXISTS iceberg.tpcds");
        return runner;
    }

    private void addWarpCatalog(DistributedQueryRunner runner, Map<String, String> icebergProperties, Path metastoreDirectory, Path warpIndexDirectory)
    {
        Map<String, String> warpProxiedProperties = new HashMap<>(icebergProperties);
        warpProxiedProperties.put("iceberg.catalog.type", "TESTING_FILE_METASTORE");
        createWarpCatalog(runner, "iceberg", warpProxiedProperties, metastoreDirectory, metastoreDirectory.resolve("iceberg"), warpIndexDirectory);
        benchmarkCatalog = WARP_CATALOG;
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
            String expected;
            if (table.equals("reason")) {
                // TODO drop the override once the benchmark dataset is regenerated with the current tpcds library
                expected = "VALUES BIGINT '55'";
            }
            else {
                expected = "SELECT count(*) FROM tpcds.sf%d.%s".formatted(scaleFactor, table);
            }
            assertThat(assertions.query("SELECT count(*) FROM iceberg.tpcds." + table))
                    .matches(expected);
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
