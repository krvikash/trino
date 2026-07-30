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
import io.airlift.units.DataSize;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.HiveMetastoreFactory;
import io.trino.plugin.iceberg.IcebergConnector;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.sql.query.QueryAssertions;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.containers.Minio;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.testing.containers.Minio.MINIO_REGION;
import static io.trino.testing.containers.Minio.MINIO_ROOT_PASSWORD;
import static io.trino.testing.containers.Minio.MINIO_ROOT_USER;
import static io.trino.tests.benchmark.BenchmarkRunner.isRemote;
import static io.trino.tests.benchmark.IcebergTablesUtil.findTableDirectory;
import static io.trino.tests.benchmark.IcebergTablesUtil.registerTables;
import static io.trino.tests.benchmark.IcebergTablesUtil.resolveTablesLocation;
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
        public List<String> defaultQueries()
        {
            return ClickBench.allQueries();
        }

        @Override
        public String readQuery(String query)
        {
            return ClickBench.readQuery(query, "iceberg", "clickbench");
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
                    1248274 metadata/20260423_182617_06324_7ypiy-ffd9fd58-cb9c-4b8d-839d-0bd8b1a5078e.stats
                    22619 metadata/00000-fea24b7c-7b9a-4e7c-b308-a5c120c77e11.metadata.json
                    23077 metadata/00002-3dbac3cd-3e83-4f73-90ef-20bd355a521b.metadata.json
                    336065 metadata/13f26731-3239-4d20-a012-39eeb4059358-m0.avro
                    350738762 data/20260423_182617_06324_7ypiy-3289be40-248d-48c4-87ab-500739f3492d.parquet
                    38267 metadata/00001-071268f6-4219-4ee0-a086-e801c4b3d1bb.metadata.json
                    38431 metadata/13f26731-3239-4d20-a012-39eeb4059358-m1.avro
                    424474942 data/20260423_182617_06324_7ypiy-f1ebe060-027e-468a-86c9-bdfc775876b9.parquet
                    429397477 data/20260423_182617_06324_7ypiy-0d4d464e-b900-4772-b861-b4b779bbc17f.parquet
                    430813350 data/20260423_182617_06324_7ypiy-8c33c6a0-9e4f-48c5-969e-6aa47f5aa599.parquet
                    432269863 data/20260423_182617_06324_7ypiy-bdb573ff-56a7-44cf-b6e4-8e2c92a6a843.parquet
                    4502 metadata/snap-8995484110236882038-1-13f26731-3239-4d20-a012-39eeb4059358.avro
                    453847814 data/20260423_182617_06324_7ypiy-355305e5-086d-4247-bd7e-b6957b05f337.parquet
                    456419928 data/20260423_182617_06324_7ypiy-22bab991-be6e-4b38-97ea-871cc85c5fcf.parquet
                    457799329 data/20260423_182617_06324_7ypiy-bc3a5c60-e2e6-4474-8fe7-9db46553a8ed.parquet
                    460068756 data/20260423_182617_06324_7ypiy-4ddc24fa-c210-45ad-ad99-2a6deb81d6c9.parquet
                    460725057 data/20260423_182617_06324_7ypiy-c2ebc1c0-0ecd-4b0d-b972-b0c33764a30c.parquet
                    463751534 data/20260423_182617_06324_7ypiy-314299b9-f6fb-453d-8841-0f1a5fc58611.parquet
                    465582301 data/20260423_182617_06324_7ypiy-8747cf98-1830-4a94-b35c-98b69ffbcf5d.parquet
                    466528252 data/20260423_182617_06324_7ypiy-266a7832-8cfc-4883-808b-ad4d6af08af4.parquet
                    467254669 data/20260423_182617_06324_7ypiy-94b92df2-eacf-48e0-a37e-606d0febcc67.parquet
                    468673535 data/20260423_182617_06324_7ypiy-267d9bf9-3deb-4b21-b5e9-b6f1a55b5efc.parquet
                    468989328 data/20260423_182617_06324_7ypiy-152ab8ef-edba-4a85-8899-e05cd40f84be.parquet
                    472867874 data/20260423_182617_06324_7ypiy-d1ebeb32-3c7d-4cdb-862a-e295fa428141.parquet
                    475263722 data/20260423_182617_06324_7ypiy-a9915ab1-22ad-4db5-8e3c-1523f34bba5c.parquet
                    476086337 data/20260423_182617_06324_7ypiy-70ac5d95-f503-440b-a9df-2ba80a20c504.parquet
                    476297655 data/20260423_182617_06324_7ypiy-7a379464-2d71-44ca-b1c9-5f4cab4f602b.parquet
                    486642343 data/20260423_182617_06324_7ypiy-07ca558a-c1ba-4b29-9fb0-154ab91b6339.parquet
                    488874496 data/20260423_182617_06324_7ypiy-5288db11-837e-43d1-931c-5d0539a14404.parquet
                    489897084 data/20260423_182617_06324_7ypiy-5d5f8d85-7c06-4c84-9d28-d75f3ccfb07c.parquet
                    492656380 data/20260423_182617_06324_7ypiy-8e7e7dff-ea7e-4b46-b2cd-1d1a4cee88a9.parquet
                    """;
            BenchmarkRunner.verifyDataListing(findTableDirectory(resolveTablesLocation(dataLocation), "hits"), "Run `testing/benchmark-data/hydrate.sh iceberg-clickbench` first.", expected);
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
            BenchmarkRunner.applyExecutionMode(builder, mode);

            Map<String, String> fsCacheProperties = new HashMap<>();
            BenchmarkRunner.applyFilesystemCache(fsCacheProperties, fsCacheDirectory);
            fsCacheProperties.forEach(builder::addIcebergProperty);

            if (mode == BenchmarkRunner.ExecutionMode.GPU) {
                builder.addIcebergProperty("iceberg.max-split-size", "512MB")
                        .addIcebergProperty("iceberg.experimental.composite-splits.enabled", "true");
            }
            if (bind8080) {
                builder.addCoordinatorProperty("http-server.http.port", "8080");
            }
            DistributedQueryRunner queryRunner = builder.build();

            queryRunner.execute("CREATE SCHEMA IF NOT EXISTS iceberg.clickbench");

            HiveMetastore hiveMetastore = ((IcebergConnector) queryRunner.getCoordinator().getConnector("iceberg")).getInjector()
                    .getInstance(HiveMetastoreFactory.class)
                    .createMetastore(Optional.empty());
            registerTables(minio, hiveMetastore, "baas-benchmark-data", dataLocation, "clickbench", List.of("hits"), "clickbench/iceberg");

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
        public String expectedResultResource(String query)
        {
            return "sql/trino/clickbench/results/%s.ndjson".formatted(query);
        }

        @Override
        public void generateData(Path target)
        {
            throw new UnsupportedOperationException("Iceberg local benchmarks use externally generated datasets.");
        }
    }
}
