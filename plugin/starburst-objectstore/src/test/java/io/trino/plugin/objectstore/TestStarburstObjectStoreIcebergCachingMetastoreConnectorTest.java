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
import io.trino.plugin.iceberg.IcebergPlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;

import java.util.Map;

import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.plugin.hive.TestingThriftHiveMetastoreBuilder.testingThriftHiveMetastoreBuilder;
import static io.trino.plugin.objectstore.StarburstObjectStoreConnectorFactory.STARBURST_OBJECTSTORE;
import static io.trino.plugin.objectstore.TableType.ICEBERG;
import static io.trino.testing.TestingSession.testSessionBuilder;

final class TestStarburstObjectStoreIcebergCachingMetastoreConnectorTest
        extends BaseObjectStoreIcebergConnectorTest
{
    public TestStarburstObjectStoreIcebergCachingMetastoreConnectorTest()
    {
        super(false);
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(
                        testSessionBuilder()
                                .setCatalog("objectstore")
                                .setSchema("tpch")
                                .build())
                .build();
        try {
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch", Map.of());

            HiveMinioStorage hiveMinio = closeAfterClass(new HiveMinioStorage(bucketName));
            hiveMinio.start();
            minio = hiveMinio.minioStorage();

            metastore = new BridgingHiveMetastore(
                    testingThriftHiveMetastoreBuilder()
                            .metastoreClient(hiveMinio.hiveMetastoreEndpoint())
                            .build(this::closeAfterClass));

            queryRunner.installPlugin(new IcebergPlugin());
            queryRunner.installPlugin(new ObjectStorePlugin());
            queryRunner.createCatalog("objectstore", STARBURST_OBJECTSTORE, ImmutableMap.<String, String>builder()
                    .put("great-lakes.table-type", ICEBERG.name())
                    .put("hive.metastore.uri", hiveMinio.hiveMetastoreEndpoint().toString())
                    .put("hive.non-managed-table-writes-enabled", "true")
                    .putAll(minio.getNativeS3Config())
                    .put("iceberg.file-format", "PARQUET")
                    .put("iceberg.add-files-procedure.enabled", "true")
                    .put("iceberg.format-version", "3")
                    .put("iceberg.register-table-procedure.enabled", "true")
                    .put("delta.enable-non-concurrent-writes", "true")
                    .put("delta.register-table-procedure.enabled", "true")
                    .put("hive.metastore-cache-ttl", "30m")
                    .buildOrThrow());
            queryRunner.execute("CREATE SCHEMA objectstore.tpch WITH (location = 's3://" + bucketName + "/tpch')");

            queryRunner.installPlugin(buildMockConnectorPlugin());
            queryRunner.createCatalog("mock_dynamic_listing", "mock", ImmutableMap.of());

            initializeTpchTables(queryRunner, metastore);
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }

        return queryRunner;
    }
}
