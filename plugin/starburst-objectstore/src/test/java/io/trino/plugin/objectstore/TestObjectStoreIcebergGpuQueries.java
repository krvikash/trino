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
import io.trino.plugin.iceberg.IcebergPlugin;
import io.trino.plugin.iceberg.TestIcebergGpuQueries;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.containers.Minio;
import org.junit.jupiter.api.Test;

import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.plugin.objectstore.ObjectStoreQueryRunner.initializeTpchTables;
import static io.trino.plugin.objectstore.StarburstObjectStoreConnectorFactory.STARBURST_OBJECTSTORE;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.containers.Minio.MINIO_REGION;
import static io.trino.testing.containers.Minio.MINIO_ROOT_PASSWORD;
import static io.trino.testing.containers.Minio.MINIO_ROOT_USER;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestObjectStoreIcebergGpuQueries
        extends TestIcebergGpuQueries
{
    private static final String BUCKET = "test-object-store-iceberg-gpu-bucket";

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

            // Register the Iceberg connector functions (e.g. the theta-sketch statistics aggregation used on write)
            queryRunner.installPlugin(new IcebergPlugin());
            queryRunner.installPlugin(new ObjectStorePlugin());
            queryRunner.createCatalog("objectstore", STARBURST_OBJECTSTORE, ImmutableMap.<String, String>builder()
                    .put("great-lakes.table-type", TableType.ICEBERG.name())
                    .put("hive.metastore", "file")
                    .put("hive.metastore.catalog.dir", "s3://" + BUCKET + "/objectstore")
                    .put("fs.s3.enabled", "true")
                    .put("s3.aws-access-key", MINIO_ROOT_USER)
                    .put("s3.aws-secret-key", MINIO_ROOT_PASSWORD)
                    .put("s3.region", MINIO_REGION)
                    .put("s3.endpoint", minio.getMinioAddress())
                    .put("s3.path-style-access", "true")
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

    @Test
    @Override
    public void testParquetFileWithoutNameMappingAndFieldIds()
    {
        // This test cannot run against object store because of how it's implemented
        assertThatThrownBy(super::testParquetFileWithoutNameMappingAndFieldIds)
                .hasMessage("Catalog 'iceberg' not found");
    }

    @Test
    @Override
    public void testUnannotatedInt64TimestampRead()
    {
        // This test cannot run against object store because of how it's implemented
        assertThatThrownBy(super::testUnannotatedInt64TimestampRead)
                .hasMessage("Catalog 'iceberg' not found");
    }

    @Test
    @Override
    public void testInt64NanosTimestampRoundingDisparity()
    {
        // This test cannot run against object store because of how it's implemented
        assertThatThrownBy(super::testInt64NanosTimestampRoundingDisparity)
                .hasMessage("Catalog 'iceberg' not found");
    }

    @Test
    @Override
    public void testInt96TimestampRead()
    {
        // This test cannot run against object store because of how it's implemented
        assertThatThrownBy(super::testInt96TimestampRead)
                .hasMessage("Catalog 'iceberg' not found");
    }

    @Test
    @Override
    public void testInt64NanosTimestampRead()
    {
        // This test cannot run against object store because of how it's implemented
        assertThatThrownBy(super::testInt64NanosTimestampRead)
                .hasMessage("Catalog 'iceberg' not found");
    }

    @Test
    @Override
    public void testParquetFileWithNameMappingAndWithoutFieldIds()
    {
        // This test cannot run against object store because of how it's implemented
        assertThatThrownBy(super::testParquetFileWithNameMappingAndWithoutFieldIds)
                .hasMessage("Catalog 'iceberg' not found");
    }
}
