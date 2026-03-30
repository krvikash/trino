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
package io.trino.plugin.iceberg.catalog.glue;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.iceberg.BaseIcebergExternalWriteTest;
import org.junit.jupiter.api.AfterAll;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.testing.SystemEnvironmentUtils.requireEnv;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.apache.iceberg.FileFormat.PARQUET;

/*
 * The test currently uses AWS Default Credential Provider Chain,
 * See https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials-chain.html#credentials-default
 * on ways to set your AWS credentials which will be needed to run this test.
 */
final class TestIcebergGlueCatalogExternalWriteTest
        extends BaseIcebergExternalWriteTest
{
    private final String bucketName = requireEnv("S3_BUCKET");
    private final String testPrefix = "test_iceberg_glue_external_write_" + randomNameSuffix();

    @AfterAll
    public void cleanupResources()
    {
        // Clean up S3 objects created during tests
        try (S3Client s3 = S3Client.create()) {
            ListObjectsV2Request listRequest = ListObjectsV2Request.builder()
                    .bucket(bucketName)
                    .prefix(testPrefix)
                    .build();
            s3.listObjectsV2Paginator(listRequest).stream()
                    .forEach(response -> {
                        List<String> keys = response.contents().stream()
                                .map(S3Object::key)
                                .collect(toImmutableList());
                        if (!keys.isEmpty()) {
                            DeleteObjectsRequest deleteRequest = DeleteObjectsRequest.builder()
                                    .bucket(bucketName)
                                    .delete(builder -> builder.objects(keys.stream()
                                            .map(key -> ObjectIdentifier.builder().key(key).build())
                                            .toList()).quiet(true))
                                    .build();
                            s3.deleteObjects(deleteRequest);
                        }
                    });
        }
    }

    @Override
    protected Map<String, String> getIcebergProperties()
    {
        return ImmutableMap.<String, String>builder()
                .put("iceberg.catalog.type", "glue")
                // Enable metastore cache shared across queries - this is where external writes can cause stale reads
                .put("iceberg.glue.metastore-cache.ttl", "30m")
                .put("fs.native-s3.enabled", "true")
                .put("hive.metastore.glue.default-warehouse-dir", "s3://%s/%s".formatted(bucketName, testPrefix))
                .put("iceberg.file-format", PARQUET.name())
                .buildOrThrow();
    }

    @Override
    protected String createSchemaSql(String schemaName)
    {
        return "CREATE SCHEMA %s WITH (location = 's3://%s/%s/%s')".formatted(schemaName, bucketName, testPrefix, schemaName);
    }
}
