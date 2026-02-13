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
package io.trino.plugin.iceberg.procedure;

import com.google.common.collect.ImmutableMap;
import io.trino.metastore.HiveMetastore;
import io.trino.plugin.hive.containers.Hive3MinioDataLake;
import io.trino.plugin.hive.metastore.thrift.BridgingHiveMetastore;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.testing.QueryRunner;

import static io.trino.plugin.hive.TestingThriftHiveMetastoreBuilder.testingThriftHiveMetastoreBuilder;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.Minio.MINIO_ACCESS_KEY;
import static io.trino.testing.containers.Minio.MINIO_REGION;
import static io.trino.testing.containers.Minio.MINIO_SECRET_KEY;

final class TestIcebergHiveFlushMetadataCacheProcedure
        extends BaseTestIcebergFlushMetadataCacheProcedure
{
    private final String bucketName = "iceberg-test-flush-metadata-cache-" + randomNameSuffix();
    private HiveMetastore metastore;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Hive3MinioDataLake hiveMinioDataLake = closeAfterClass(new Hive3MinioDataLake(bucketName));
        hiveMinioDataLake.start();
        metastore = new BridgingHiveMetastore(
                testingThriftHiveMetastoreBuilder()
                        .metastoreClient(hiveMinioDataLake.getHiveMetastoreEndpoint())
                        .build(this::closeAfterClass));

        return IcebergQueryRunner.builder("default")
                .setIcebergProperties(ImmutableMap.<String, String>builder()
                        .put("iceberg.catalog.type", "HIVE_METASTORE")
                        .put("hive.metastore.uri", hiveMinioDataLake.getHiveMetastoreEndpoint().toString())
                        .put("hive.metastore.thrift.client.read-timeout", "1m")
                        .put("hive.metastore-cache-ttl", "10m")
                        .put("fs.native-s3.enabled", "true")
                        .put("s3.aws-access-key", MINIO_ACCESS_KEY)
                        .put("s3.aws-secret-key", MINIO_SECRET_KEY)
                        .put("s3.region", MINIO_REGION)
                        .put("s3.endpoint", hiveMinioDataLake.getMinio().getMinioAddress())
                        .put("s3.path-style-access", "true")
                        .buildOrThrow())
                .build();
    }

    @Override
    protected String getSchemaLocation()
    {
        return "s3://" + bucketName;
    }

    @Override
    protected void renameTableOutsideTrino(String schemaName, String sourceTableName, String targetTableName)
    {
        metastore.renameTable(schemaName, sourceTableName, schemaName, targetTableName);
    }
}
