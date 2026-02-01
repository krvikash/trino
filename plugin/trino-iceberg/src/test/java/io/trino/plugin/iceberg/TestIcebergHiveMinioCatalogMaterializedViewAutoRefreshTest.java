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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.hive.containers.Hive3MinioDataLake;
import io.trino.plugin.hive.containers.HiveHadoop;
import io.trino.testing.QueryRunner;
import org.apache.iceberg.FileFormat;

import java.util.Map;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.Minio.MINIO_ROOT_PASSWORD;
import static io.trino.testing.containers.Minio.MINIO_ROOT_USER;

public class TestIcebergHiveMinioCatalogMaterializedViewAutoRefreshTest
        extends TestIcebergHiveCatalogMaterializedViewAutoRefreshTest
{
    private Hive3MinioDataLake hive3MinioDataLake;
    private String bucketName;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        this.bucketName = "test-iceberg-hive-minio-mv-auto-refresh-test-" + randomNameSuffix();
        hive3MinioDataLake = closeAfterClass(new Hive3MinioDataLake(bucketName, HiveHadoop.HIVE3_IMAGE));
        hive3MinioDataLake.start();
        return super.createQueryRunner();
    }

    @Override
    protected Map<String, String> getIcebergCatalogProperties()
    {
        return ImmutableMap.<String, String>builder()
                .put("iceberg.catalog.type", "HIVE_METASTORE")
                .put("hive.metastore.uri", hive3MinioDataLake.getHiveHadoop().getHiveMetastoreEndpoint().toString())
                .put("fs.hadoop.enabled", "false")
                .put("fs.native-s3.enabled", "true")
                .put("s3.aws-access-key", MINIO_ROOT_USER)
                .put("s3.aws-secret-key", MINIO_ROOT_PASSWORD)
                .put("s3.endpoint", hive3MinioDataLake.getMinio().getMinioAddress())
                .put("s3.region", "us-east-1")
                .put("s3.path-style-access", "true")
                .put("iceberg.file-format", FileFormat.PARQUET.name())
                .put("iceberg.register-table-procedure.enabled", "true")
                .put("iceberg.writer-sort-buffer-size", "1MB")
                .buildOrThrow();
    }

    @Override
    protected String createSchemaSql(String catalog, String schemaName)
    {
        return "CREATE SCHEMA %s.%s WITH (location = 's3://%s/%s')".formatted(catalog, schemaName, bucketName, schemaName);
    }
}
