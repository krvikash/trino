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
import io.trino.plugin.hive.containers.Hive3FlociDataLake;
import io.trino.plugin.hive.containers.HiveHadoop;
import io.trino.testing.QueryRunner;
import org.apache.iceberg.FileFormat;

import java.util.Map;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.Floci.FLOCI_ACCESS_KEY;
import static io.trino.testing.containers.Floci.FLOCI_REGION;
import static io.trino.testing.containers.Floci.FLOCI_SECRET_KEY;

public class TestIcebergHiveMinioCatalogMaterializedViewAutoRefreshTest
        extends TestIcebergHiveCatalogMaterializedViewAutoRefreshTest
{
    private Hive3FlociDataLake hive3FlociDataLake;
    private String bucketName;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        this.bucketName = "test-iceberg-hive-minio-mv-auto-refresh-test-" + randomNameSuffix();
        hive3FlociDataLake = closeAfterClass(new Hive3FlociDataLake(bucketName, HiveHadoop.HIVE3_IMAGE));
        hive3FlociDataLake.start();
        return super.createQueryRunner();
    }

    @Override
    protected Map<String, String> getIcebergCatalogProperties()
    {
        return ImmutableMap.<String, String>builder()
                .put("iceberg.catalog.type", "HIVE_METASTORE")
                .put("hive.metastore.uri", hive3FlociDataLake.getHiveHadoop().getHiveMetastoreEndpoint().toString())
                .put("fs.hadoop.enabled", "false")
                .put("fs.s3.enabled", "true")
                .put("s3.aws-access-key", FLOCI_ACCESS_KEY)
                .put("s3.aws-secret-key", FLOCI_SECRET_KEY)
                .put("s3.endpoint", hive3FlociDataLake.floci().endpoint().toString())
                .put("s3.region", FLOCI_REGION)
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
