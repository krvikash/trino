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
import io.trino.plugin.hive.metastore.thrift.BridgingHiveMetastore;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.plugin.hive.TestingThriftHiveMetastoreBuilder.testingThriftHiveMetastoreBuilder;
import static io.trino.plugin.objectstore.StarburstObjectStoreConnectorFactory.STARBURST_OBJECTSTORE;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.containers.Minio.MINIO_REGION;
import static io.trino.testing.containers.Minio.MINIO_ROOT_PASSWORD;
import static io.trino.testing.containers.Minio.MINIO_ROOT_USER;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestStarburstObjectStoreHiveOnDataLake
        extends BaseTestObjectStoreHiveOnDataLake
{
    private static final String BUCKET_NAME = "test-object-store-on-data-lake-" + randomNameSuffix();

    public TestStarburstObjectStoreHiveOnDataLake()
    {
        super(BUCKET_NAME);
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        this.hiveMinioDataLake.start();
        metastoreClient = new BridgingHiveMetastore(
                testingThriftHiveMetastoreBuilder()
                        .metastoreClient(hiveMinioDataLake.getHiveHadoop().getHiveMetastoreEndpoint())
                        .build(this::closeAfterClass));

        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(
                        testSessionBuilder()
                                .setCatalog("hive")
                                .setSchema("tpch")
                                .build())
                .build();
        try {
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");

            queryRunner.installPlugin(new ObjectStorePlugin());
            queryRunner.createCatalog("hive", STARBURST_OBJECTSTORE, ImmutableMap.<String, String>builder()
                    .put("great-lakes.table-type", TableType.HIVE.name())
                    .put("fs.hadoop.enabled", "false")
                    .put("fs.s3.enabled", "true")
                    .put("s3.aws-access-key", MINIO_ROOT_USER)
                    .put("s3.aws-secret-key", MINIO_ROOT_PASSWORD)
                    .put("s3.region", MINIO_REGION)
                    .put("s3.endpoint", hiveMinioDataLake.getMinio().getMinioAddress())
                    .put("s3.path-style-access", "true")
                    // Metastore
                    .put("hive.metastore.uri", hiveMinioDataLake.getHiveHadoop().getHiveMetastoreEndpoint().toString())
                    // Required for tests
                    .put("hive.insert-existing-partitions-behavior", "OVERWRITE")
                    .put("hive.hive-views.enabled", "true")
                    .put("hive.non-managed-table-writes-enabled", "true")
                    // Below are required to enable caching on metastore (as enabled by the superclass)
                    .put("hive.metastore-cache-ttl", "1d")
                    .put("hive.metastore-refresh-interval", "1d")
                    .buildOrThrow());

            queryRunner.execute("CREATE SCHEMA hive.tpch");
            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    @Override
    boolean isGalaxyMetastore()
    {
        return false;
    }

    @Test
    @Override
    public void testInsertOverwriteInTransaction()
    {
        assertThatThrownBy(super::testInsertOverwriteInTransaction)
                .hasMessageContaining("Catalog only supports writes using autocommit: hive");
    }
}
