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

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.trino.Session;
import io.trino.plugin.hive.TestingHivePlugin;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.sql.query.QueryAssertions;
import io.trino.testing.DistributedQueryRunner;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;

import static com.google.common.io.Resources.getResource;
import static io.trino.tests.benchmark.BenchmarkRunner.applyDataGenerationConfiguration;
import static io.trino.tests.benchmark.IcebergTableDirectoryFinder.findTableDirectory;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Iceberg ClickBench entry point.
 */
public final class BenchmarkIcebergClickBench
{
    private BenchmarkIcebergClickBench() {}

    private static final Logger log = Logger.get(BenchmarkIcebergClickBench.class);

    static void main(String[] args)
            throws Exception
    {
        System.exit(BenchmarkRunner.run(args, new IcebergClickBenchWorkload(), BenchmarkIcebergClickBench.class));
    }

    static final class IcebergClickBenchWorkload
            implements Workload
    {
        @Override
        public DataSize jvmHeapSize()
        {
            return DataSize.of(10, DataSize.Unit.GIGABYTE);
        }

        @Override
        public String name()
        {
            return "iceberg-clickbench";
        }

        @Override
        public List<Integer> defaultQueries()
        {
            return IntStream.rangeClosed(0, 42).boxed().toList();
        }

        @Override
        public String readQuery(int queryNumber)
        {
            try {
                return Resources.toString(
                                getResource("sql/trino/clickbench/queries/q%02d.sql".formatted(queryNumber)), UTF_8)
                        .replace("${database}", "iceberg")
                        .replace("${schema}", "clickbench")
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
            String expected =
                    """
                    1067716395 data/20260605_123413_00004_sag53-417c1372-5792-4909-8e78-a385fb699f4a.parquet
                    1068607614 data/20260605_123413_00004_sag53-d7e0e11e-c2d0-453b-837e-42f3db06588e.parquet
                    1068725805 data/20260605_123413_00004_sag53-7c287432-e169-4771-9068-ac6373432276.parquet
                    1068898126 data/20260605_123413_00004_sag53-a4126fb5-02ab-4e05-8cbf-64f030ee79ca.parquet
                    1069252707 data/20260605_123413_00004_sag53-e17df480-0024-4641-bb37-5c75237db258.parquet
                    1069823059 data/20260605_123413_00004_sag53-d290f615-65cc-433b-a27c-613cbd643d01.parquet
                    1069950674 data/20260605_123413_00004_sag53-7398353c-cab8-4639-be84-8e269cc5efe3.parquet
                    1070096931 data/20260605_123413_00004_sag53-909575f7-6149-46a9-a5d8-565c7e15341b.parquet
                    1070102906 data/20260605_123413_00004_sag53-a8e65d52-0a8d-4b75-bd24-323ad0928acf.parquet
                    1070587202 data/20260605_123413_00004_sag53-b4faa626-d24d-4fb4-8451-de96fc52d63a.parquet
                    1071031426 data/20260605_123413_00004_sag53-0ecef26b-4bc8-401a-b0f2-35d939f3701f.parquet
                    1071522786 data/20260605_123413_00004_sag53-c37ee053-6e5a-4370-b39f-93652ce20bbd.parquet
                    1071894342 data/20260605_123413_00004_sag53-90434143-3544-4247-9983-a95a80fd0eeb.parquet
                    1072303929 data/20260605_123413_00004_sag53-b55d10fa-6546-4522-8fab-55000b3980ea.parquet
                    1072865957 data/20260605_123413_00004_sag53-0c32b988-9789-4576-95ab-051b3019c5a5.parquet
                    1246581 metadata/20260605_123413_00004_sag53-c2860e55-4b6d-404f-abfe-7ccc3fbe12ba.stats
                    23626 metadata/00001-547595c5-fd5c-4ab1-9daf-9a20cc966aaa.metadata.json
                    29836 metadata/38afb0f2-9b22-4e9c-bc51-305020379003-m0.avro
                    341218930 data/20260605_123413_00004_sag53-9d0ba535-b984-403d-8722-60f645529f84.parquet
                    4313 metadata/snap-2289599665082482433-1-99edc5a6-cda5-461e-b80d-91a24d289a8c.avro
                    4485 metadata/snap-4400031080874046512-1-38afb0f2-9b22-4e9c-bc51-305020379003.avro
                    8040 metadata/00000-2501ff6c-1201-469c-9790-aa5fc04dad1e.metadata.json
                    """;
            BenchmarkRunner.verifyDataListing(findTableDirectory(resolveTablesLocation(dataLocation), "hits"), "Run `testing/benchmark-data/hydrate.sh iceberg-clickbench` first.", expected);
        }

        @Override
        public DistributedQueryRunner createRunner(String dataLocation, BenchmarkRunner.ExecutionMode mode, boolean bind8080, Optional<Path> rmmLogPath)
                throws Exception
        {
            IcebergQueryRunner.Builder builder = IcebergQueryRunner.builder()
                    .setMetastoreDirectory(Path.of(dataLocation).toFile())
                    .setWorkerCount(0)
                    .disableSchemaInitializer()
                    .addIcebergProperty("iceberg.register-table-procedure.enabled", "true");
            BenchmarkRunner.applyExecutionMode(builder, mode);
            if (bind8080) {
                builder.addCoordinatorProperty("http-server.http.port", "8080");
            }
            rmmLogPath.ifPresent(path -> builder.setAdditionalModule(new RmmLoggingModule(path)));
            DistributedQueryRunner queryRunner = builder.build();

            queryRunner.execute("CREATE SCHEMA IF NOT EXISTS iceberg.clickbench");
            registerTable(queryRunner, dataLocation);

            return queryRunner;
        }

        @Override
        public void verifyDataset(DistributedQueryRunner runner)
        {
            log.info("Verifying row count: iceberg.clickbench.hits");
            assertThat(new QueryAssertions(runner).query("SELECT count(*) FROM iceberg.clickbench.hits"))
                    .matches("VALUES BIGINT '99997497'");
        }

        @Override
        public List<String> tablesForStats()
        {
            return List.of();
        }

        @Override
        public String expectedResultResource(int queryNumber)
        {
            return "sql/trino/clickbench/results/q%02d.ndjson".formatted(queryNumber);
        }

        @Override
        public void generateData(Path target)
                throws Exception
        {
            Path hiveSource = hiveDataLocation();
            if (!Files.isDirectory(hiveSource)) {
                throw new IllegalStateException("Hive source data not found at " + hiveSource + ". Run `testing/benchmark-data/hydrate.sh hive-clickbench` first.");
            }

            Path tablesLocation = resolveTablesLocation(target.toAbsolutePath().toString());
            try (DistributedQueryRunner runner = applyDataGenerationConfiguration(IcebergQueryRunner.builder())
                    .disableSchemaInitializer()
                    .setMetastoreDirectory(target.toFile())
                    .addIcebergProperty("iceberg.compression-codec", "SNAPPY")
                    .addIcebergProperty("parquet.writer.page-value-count", "100000")
                    .build()) {
                runner.installPlugin(new TestingHivePlugin(runner.getCoordinator().getBaseDataDir()));
                runner.createCatalog("hive", "hive", ImmutableMap.of(
                        "hive.parquet.time-zone", "UTC",
                        "hive.metastore.disable-location-checks", "true",
                        "fs.hadoop.enabled", "true"));

                Session session = BenchmarkRunner.withSingleWriter(runner.getDefaultSession());

                String sourcePath = hiveSource.toAbsolutePath().normalize().toUri().toString();
                runner.execute("CREATE SCHEMA hive.clickbench_src");
                runner.execute(format(
                        "CREATE TABLE hive.clickbench_src.hits (%s) WITH (external_location = '%s', format = 'PARQUET')",
                        BenchmarkHiveClickBench.HITS_COLUMNS,
                        sourcePath));

                String schemaLocation = target.toAbsolutePath().normalize()
                        .relativize(tablesLocation.toAbsolutePath().normalize())
                        .toString();
                runner.execute(session, "CREATE SCHEMA iceberg.clickbench WITH (location = 'local:///%s')".formatted(schemaLocation));
                // CREATE TABLE implicitly widens SMALLINT to INTEGER (Iceberg has no SMALLINT),
                // so we can reuse the Hive column definitions directly.
                runner.execute(session, format("CREATE TABLE iceberg.clickbench.hits (%s) WITH (format = 'PARQUET')", BenchmarkHiveClickBench.HITS_COLUMNS));
                log.info("Generating iceberg clickbench.hits");
                runner.execute(session, "INSERT INTO iceberg.clickbench.hits SELECT * FROM hive.clickbench_src.hits");
            }
            BenchmarkRunner.cleanCrcFiles(target);
        }

        private static Path hiveDataLocation()
        {
            return Path.of(new BenchmarkHiveClickBench.ClickBenchWorkload().defaultDataLocation(), "hits");
        }

        private static void registerTable(DistributedQueryRunner runner, String dataLocation)
        {
            long tableCount = (Long) runner.execute(
                            "SELECT count(*) FROM iceberg.information_schema.tables WHERE table_schema = 'clickbench' AND table_name = 'hits'")
                    .getOnlyValue();
            if (tableCount > 0) {
                log.info("Reusing existing iceberg.clickbench.hits");
                return;
            }
            Path tableDir = findTableDirectory(resolveTablesLocation(dataLocation), "hits");
            String relativePath = Path.of(dataLocation).toAbsolutePath().normalize()
                    .relativize(tableDir.toAbsolutePath().normalize())
                    .toString();
            String location = "local:///" + relativePath;
            log.info("Registering iceberg.clickbench.hits at %s", location);
            runner.execute(format("CALL iceberg.system.register_table('clickbench', 'hits', '%s')", location));
        }

        static Path resolveTablesLocation(String dataLocation)
        {
            return Path.of(dataLocation, "tables");
        }
    }
}
