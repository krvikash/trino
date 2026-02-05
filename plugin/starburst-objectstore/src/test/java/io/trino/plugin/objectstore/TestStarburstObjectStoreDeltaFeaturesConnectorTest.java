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
import io.trino.metastore.HiveMetastoreFactory;
import io.trino.plugin.deltalake.DeltaLakeConnector;
import io.trino.plugin.deltalake.DeltaLakeQueryRunner;
import io.trino.plugin.hive.containers.Hive3MinioDataLake;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;

import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.plugin.objectstore.ObjectStoreQueryRunner.initializeTpchTables;
import static io.trino.plugin.objectstore.StarburstObjectStoreConnectorFactory.STARBURST_OBJECTSTORE;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.TransactionBuilder.transaction;
import static io.trino.testing.containers.Minio.MINIO_ACCESS_KEY;
import static io.trino.testing.containers.Minio.MINIO_REGION;
import static io.trino.testing.containers.Minio.MINIO_SECRET_KEY;

public class TestStarburstObjectStoreDeltaFeaturesConnectorTest
        extends BaseTestObjectStoreDeltaFeaturesConnectorTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Hive3MinioDataLake hiveMinioDataLake = closeAfterClass(new Hive3MinioDataLake(bucketName));
        hiveMinioDataLake.start();
        minioClient = closeAfterClass(hiveMinioDataLake.getMinioClient());

        String catalog = DeltaLakeQueryRunner.DELTA_CATALOG;
        String schema = "test_schema"; // must match TestDeltaLakeConnectorTest.SCHEMA
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(
                        testSessionBuilder()
                                .setCatalog(catalog)
                                .setSchema(schema)
                                .build())
                .build();
        try {
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch", Map.of());

            queryRunner.installPlugin(new ObjectStorePlugin());
            queryRunner.createCatalog(catalog, STARBURST_OBJECTSTORE, ImmutableMap.<String, String>builder()
                    .put("hive.metastore.uri", hiveMinioDataLake.getHiveMetastoreEndpoint().toString())
                    .put("hive.metastore.thrift.client.read-timeout", "1m") // read timed out sometimes happens with the default timeout
                    .put("delta.register-table-procedure.enabled", "true")
                    .put("delta.metastore.store-table-metadata", "true")
                    .put("delta.metastore.store-table-metadata-threads", "0")
                    .put("fs.native-s3.enabled", "true")
                    .put("s3.aws-access-key", MINIO_ACCESS_KEY)
                    .put("s3.aws-secret-key", MINIO_SECRET_KEY)
                    .put("s3.region", MINIO_REGION)
                    .put("s3.endpoint", hiveMinioDataLake.getMinio().getMinioAddress())
                    .put("s3.path-style-access", "true")
                    .put("s3.streaming.part-size", "5MB") // minimize memory usage
                    .put("great-lakes.table-type", TableType.DELTA.name())
                    .buildOrThrow());

            queryRunner.execute("CREATE SCHEMA %1$s.%2$s WITH (location = 's3://%3$s/%2$s')".formatted(catalog, schema, bucketName));
            initializeTpchTables(queryRunner, REQUIRED_TPCH_TABLES);

            ObjectStoreConnector objectStoreConnector = transaction(queryRunner.getTransactionManager(), queryRunner.getPlannerContext().getMetadata(), queryRunner.getAccessControl())
                    .execute(queryRunner.getDefaultSession(), transactionSession -> (ObjectStoreConnector) queryRunner.getCoordinator().getConnector(transactionSession, catalog));
            DeltaLakeConnector deltaLakeConnector = (DeltaLakeConnector) objectStoreConnector.getInjector().getInstance(DelegateConnectors.class).deltaConnector();
            metastore = deltaLakeConnector.getInjector().getInstance(HiveMetastoreFactory.class).createMetastore(Optional.empty());

            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }
}
