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
import io.trino.Session;
import io.trino.plugin.hive.containers.Hive3FlociDataLake;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.plugin.iceberg.SchemaInitializer;
import io.trino.spi.security.Identity;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.Floci.FLOCI_ACCESS_KEY;
import static io.trino.testing.containers.Floci.FLOCI_REGION;
import static io.trino.testing.containers.Floci.FLOCI_SECRET_KEY;
import static org.assertj.core.api.Assertions.assertThat;

final class TestIcebergFlushMetadataCacheWithImpersonation
        extends AbstractTestQueryFramework
{
    private final String bucketName = "iceberg-test-impersonation-" + randomNameSuffix();
    private final String schemaName = "test_schema_" + randomNameSuffix();

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Hive3FlociDataLake hiveFlociDataLake = closeAfterClass(new Hive3FlociDataLake(bucketName));
        hiveFlociDataLake.start();

        return IcebergQueryRunner.builder()
                .setIcebergProperties(ImmutableMap.<String, String>builder()
                        .put("iceberg.catalog.type", "HIVE_METASTORE")
                        .put("hive.metastore.uri", hiveFlociDataLake.getHiveMetastoreEndpoint().toString())
                        .put("hive.metastore.thrift.client.read-timeout", "1m")
                        .put("hive.metastore.thrift.impersonation.enabled", "true")
                        .put("hive.metastore-cache-ttl", "1d")
                        .put("hive.user-metastore-cache-ttl", "1d")
                        .put("fs.s3.enabled", "true")
                        .put("s3.aws-access-key", FLOCI_ACCESS_KEY)
                        .put("s3.aws-secret-key", FLOCI_SECRET_KEY)
                        .put("s3.region", FLOCI_REGION)
                        .put("s3.endpoint", hiveFlociDataLake.floci().endpoint().toString())
                        .put("s3.path-style-access", "true")
                        .buildOrThrow())
                .setSchemaInitializer(
                        SchemaInitializer.builder()
                                .withSchemaName(schemaName)
                                .withClonedTpchTables(TpchTable.getTables())
                                .withSchemaProperties(Map.of("location", "'s3://" + bucketName + "/" + schemaName + "'"))
                                .build())
                .build();
    }

    @Test
    void testFlushMetadataCache()
    {
        Session alice = Session.builder(getSession()).setIdentity(Identity.ofUser("alice")).build();

        String tableName = "test_flush_cache_impersonation_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM tpch.tiny.nation", 25);

        try {
            // Alice queries the table - her cache is populated
            assertThat(computeScalar(alice, "SELECT count(*) FROM " + tableName))
                    .isEqualTo(25L);

            // Default user inserts more data
            assertUpdate("INSERT INTO " + tableName + " SELECT * FROM tpch.tiny.nation", 25);

            // Alice still sees old count from her cache
            assertThat(computeScalar(alice, "SELECT count(*) FROM " + tableName))
                    .isEqualTo(25L);

            // Alice flushes her cache
            assertUpdate(alice, "CALL system.flush_metadata_cache(schema_name => CURRENT_SCHEMA, table_name => '" + tableName + "')");

            // Alice now sees the updated count
            assertThat(computeScalar(alice, "SELECT count(*) FROM " + tableName))
                    .isEqualTo(50L);
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }
}
