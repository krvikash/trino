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
package io.trino.plugin.hive;

import com.google.common.collect.ImmutableMap;
import io.trino.filesystem.Location;
import io.trino.testing.QueryRunner;
import io.trino.testing.containers.Minio;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.Minio.MINIO_REGION;
import static io.trino.testing.containers.Minio.MINIO_ROOT_PASSWORD;
import static io.trino.testing.containers.Minio.MINIO_ROOT_USER;

public class TestHiveGpuS3MinioQueries
        extends BaseHiveGpuQueriesTest
{
    private static final String BUCKET = "test-hive-gpu-bucket";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Minio minio = closeAfterClass(Minio.builder().build());
        minio.start();
        minio.createBucket(BUCKET);

        return HiveQueryRunner.builder()
                .addExtraProperty("gpu-execution", "true")
                .addExtraProperty("task.gpu-execution.enabled", "true")
                // GPU is disabled on coordinator unless include-coordinator is set. Disable include-coordinator to force coordinator into more production-like setup.
                // This is needed to expose potential problems where operators on workers and coordinator do not match.
                .addExtraProperty("node-scheduler.include-coordinator", "false")
                .setHiveProperties(ImmutableMap.<String, String>builder()
                        .put("fs.native-s3.enabled", "true")
                        .put("s3.aws-access-key", MINIO_ROOT_USER)
                        .put("s3.aws-secret-key", MINIO_ROOT_PASSWORD)
                        .put("s3.region", MINIO_REGION)
                        .put("s3.endpoint", minio.getMinioAddress())
                        .put("s3.path-style-access", "true")
                        .put("hive.non-managed-table-writes-enabled", "true")
                        .put("hive.metastore", "file")
                        .put("hive.metastore.catalog.dir", "s3://%s/catalog".formatted(BUCKET))
                        .put("hive.storage-format", "PARQUET")
                        .put("hive.parquet.time-zone", "UTC")
                        .buildOrThrow())
                .build();
    }

    @Override
    protected Location newExternalTableLocation()
    {
        return Location.of("s3://%s/gpu_test_%s".formatted(BUCKET, randomNameSuffix()));
    }
}
