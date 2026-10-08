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
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.testing.SystemEnvironmentUtils.requireEnv;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

/**
 * Verifies that the Hive connector can read and write data through an S3 access point alias used in place of a bucket name.
 */
@TestInstance(PER_CLASS)
final class TestHiveS3AccessPoint
        extends AbstractTestQueryFramework
{
    private final String accessPointAlias = requireEnv("S3_ACCESS_POINT_ALIAS");
    private final String schemaName = "test_access_point_" + randomNameSuffix();

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = HiveQueryRunner.builder(schemaName)
                .setHiveProperties(ImmutableMap.<String, String>builder()
                        .put("hive.metastore", "file")
                        .put("hive.metastore.catalog.dir", "s3://%s/hive-access-point-test/".formatted(accessPointAlias))
                        .put("fs.s3.enabled", "true")
                        .put("hive.non-managed-table-writes-enabled", "true")
                        .put("hive.security", "allow-all")
                        .put("s3.region", requireEnv("AWS_REGION"))
                        .buildOrThrow())
                .setCreateTpchSchemas(false)
                .build();
        queryRunner.execute("CREATE SCHEMA " + schemaName);
        return queryRunner;
    }

    @AfterAll
    void dropSchema()
    {
        getQueryRunner().execute("DROP SCHEMA IF EXISTS " + schemaName + " CASCADE");
    }

    @Test
    void testReadWriteThroughAccessPoint()
    {
        String tableName = "test_table_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 1 id, 'one' name", 1);
        try {
            assertUpdate("INSERT INTO " + tableName + " VALUES (2, 'two')", 1);
            assertQuery("SELECT * FROM " + tableName, "VALUES (1, 'one'), (2, 'two')");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    void testReadWriteExternalTableThroughAccessPoint()
    {
        String tableSuffix = randomNameSuffix();
        String tableName = "test_external_table_" + tableSuffix;
        String prefix = "hive-access-point-test/external/" + tableSuffix;
        String location = "s3://%s/%s".formatted(accessPointAlias, prefix);
        assertUpdate("CREATE TABLE " + tableName + " WITH (external_location = '" + location + "') AS SELECT 1 id, 'one' name", 1);
        try {
            assertThat((String) computeScalar("SHOW CREATE TABLE " + tableName)).contains("external_location = '" + location + "'");
            assertUpdate("INSERT INTO " + tableName + " VALUES (2, 'two')", 1);
            assertQuery("SELECT * FROM " + tableName, "VALUES (1, 'one'), (2, 'two')");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
            // Dropping an external table leaves its files behind
            deleteObjects(prefix);
        }
    }

    private void deleteObjects(String prefix)
    {
        try (S3Client s3 = S3Client.builder().build()) {
            List<ObjectIdentifier> objects = s3.listObjectsV2Paginator(request -> request.bucket(accessPointAlias).prefix(prefix)).contents().stream()
                    .map(object -> ObjectIdentifier.builder().key(object.key()).build())
                    .collect(toImmutableList());
            if (!objects.isEmpty()) {
                s3.deleteObjects(request -> request.bucket(accessPointAlias).delete(delete -> delete.objects(objects)));
            }
        }
    }
}
