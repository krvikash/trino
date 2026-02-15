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
package io.trino.plugin.iceberg.catalog.hms;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.hive.containers.Hive3MinioDataLake;
import io.trino.plugin.hive.containers.HiveHadoop;
import io.trino.plugin.iceberg.BaseIcebergExternalWriteTest;
import org.junit.jupiter.api.AfterAll;

import java.util.Map;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.Minio.MINIO_ACCESS_KEY;
import static io.trino.testing.containers.Minio.MINIO_SECRET_KEY;
import static org.apache.iceberg.FileFormat.PARQUET;

final class TestIcebergHiveCatalogExternalWriteTest
        extends BaseIcebergExternalWriteTest
{
    private final String bucketName = "test-iceberg-hms-concurrent-" + randomNameSuffix();
    private final Hive3MinioDataLake hive3MinioDataLake;

    public TestIcebergHiveCatalogExternalWriteTest()
    {
        hive3MinioDataLake = new Hive3MinioDataLake(bucketName, HiveHadoop.HIVE3_IMAGE);
        hive3MinioDataLake.start();
    }

    @AfterAll
    public void cleanupResources()
            throws Exception
    {
        hive3MinioDataLake.close();
    }

    @Override
    protected Map<String, String> getIcebergProperties()
    {
        return ImmutableMap.<String, String>builder()
                .put("iceberg.catalog.type", "HIVE_METASTORE")
                .put("hive.metastore.uri", hive3MinioDataLake.getHiveHadoop().getHiveMetastoreEndpoint().toString())
                .put("fs.hadoop.enabled", "false")
                .put("fs.native-s3.enabled", "true")
                .put("s3.aws-access-key", MINIO_ACCESS_KEY)
                .put("s3.aws-secret-key", MINIO_SECRET_KEY)
                .put("s3.endpoint", hive3MinioDataLake.getMinio().getMinioAddress())
                .put("s3.region", "us-east-1")
                .put("s3.path-style-access", "true")
                .put("iceberg.file-format", PARQUET.name())
                .buildOrThrow();
    }

    @Override
    protected String createSchemaSql(String schemaName)
    {
        return "CREATE SCHEMA %s WITH (location = 's3://%s/%s')".formatted(schemaName, bucketName, schemaName);
    }
}
