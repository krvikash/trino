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
package io.trino.plugin.objectstore;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.deltalake.TestDeltaLakeGpuQueries;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.containers.Minio;

import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.plugin.objectstore.ObjectStoreQueryRunner.initializeTpchTables;
import static io.trino.plugin.objectstore.StarburstObjectStoreConnectorFactory.STARBURST_OBJECTSTORE;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.containers.Minio.MINIO_REGION;
import static io.trino.testing.containers.Minio.MINIO_ROOT_PASSWORD;
import static io.trino.testing.containers.Minio.MINIO_ROOT_USER;

class TestObjectStoreDeltaGpuQueries
        extends TestDeltaLakeGpuQueries
{
    private static final String BUCKET = "test-object-store-delta-gpu-bucket";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(
                        testSessionBuilder()
                                .setCatalog("objectstore")
                                .setSchema("tpch")
                                .build())
                .configureGpuDistributedExecution()
                .build();
        try {
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");

            Minio minio = closeAfterClass(Minio.builder().build());
            minio.start();
            minio.createBucket(BUCKET);

            queryRunner.installPlugin(new ObjectStorePlugin());
            queryRunner.createCatalog("objectstore", STARBURST_OBJECTSTORE, ImmutableMap.<String, String>builder()
                    .put("great-lakes.table-type", TableType.DELTA.name())
                    .put("hive.metastore", "file")
                    .put("hive.metastore.catalog.dir", "s3://" + BUCKET + "/objectstore")
                    .put("fs.s3.enabled", "true")
                    .put("s3.aws-access-key", MINIO_ROOT_USER)
                    .put("s3.aws-secret-key", MINIO_ROOT_PASSWORD)
                    .put("s3.region", MINIO_REGION)
                    .put("s3.endpoint", minio.getMinioAddress())
                    .put("s3.path-style-access", "true")
                    .put("delta.enable-non-concurrent-writes", "true")
                    // delta.parquet.time-zone defaults to the JVM zone; pin UTC so Delta timestamps stay GPU-eligible
                    .put("delta.parquet.time-zone", "UTC")
                    .buildOrThrow());
            queryRunner.execute("CREATE SCHEMA objectstore.tpch");
            initializeTpchTables(queryRunner, REQUIRED_TPCH_TABLES);
            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }
}
