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
package io.trino.tests.product.objectstore;

import io.trino.tempto.BeforeMethodWithContext;
import io.trino.tempto.ProductTest;
import org.testng.annotations.Test;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tests.product.TestGroups.OBJECTSTORE;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static org.assertj.core.api.Assertions.assertThat;

public class TestObjectStore
        extends ProductTest
{
    private static final String S3_BUCKET_NAME = "test-bucket";

    @BeforeMethodWithContext
    public void setup()
    {
        onTrino().executeQuery("CREATE SCHEMA IF NOT EXISTS minio.test WITH (location = 's3://%s/test')".formatted(S3_BUCKET_NAME));
    }

    @Test(groups = {OBJECTSTORE, PROFILE_SPECIFIC_TESTS})
    public void testCreateTableAsSelect()
    {
        testCreateTableAsSelect("minio", "s3://" + S3_BUCKET_NAME);
        testCreateTableAsSelect("hdfs", "hdfs://hadoop-master:9000/user/hive/warehouse");
    }

    private void testCreateTableAsSelect(String catalogName, String location)
    {
        String schemaName = "test_schema_" + randomNameSuffix();
        String tableName = "test_table_" + randomNameSuffix();

        onTrino().executeQuery("CREATE SCHEMA %1$s.%2$s WITH (location = '%3$s/%2$s')".formatted(catalogName, schemaName, location));
        onTrino().executeQuery("USE %s.%s".formatted(catalogName, schemaName));
        try {
            onTrino().executeQuery("CREATE TABLE " + schemaName + "." + tableName + " AS SELECT * FROM tpch.tiny.nation");
            assertThat(onTrino().executeQuery("SELECT * FROM " + schemaName + "." + tableName).rows())
                    .containsExactlyInAnyOrderElementsOf(onTrino().executeQuery("SELECT * FROM tpch.tiny.nation").rows());

            assertThat((String) onTrino().executeQuery("SELECT \"$path\" FROM " + schemaName + "." + tableName + " LIMIT 1").getOnlyValue())
                    .startsWith(location);
        }
        finally {
            onTrino().executeQuery("DROP TABLE IF EXISTS " + schemaName + "." + tableName);
            onTrino().executeQuery("DROP SCHEMA " + schemaName);
        }
    }

    @Test(groups = {OBJECTSTORE, PROFILE_SPECIFIC_TESTS})
    public void testIcebergDeletionVector()
    {
        onTrino().executeQuery("USE minio.test");
        String tableName = "test_iceberg_deletion_vector" + randomNameSuffix();
        onTrino().executeQuery("CREATE TABLE " + tableName + " WITH (type = 'ICEBERG', format_version = 3) AS SELECT * FROM tpch.tiny.region");
        try {
            assertThat(onTrino().executeQuery("SELECT * FROM " + tableName).rows())
                    .hasSize(5)
                    .containsExactlyInAnyOrderElementsOf(onTrino().executeQuery("SELECT * FROM tpch.tiny.region").rows());

            onTrino().executeQuery("DELETE FROM " + tableName + " WHERE regionkey = 0");
            assertThat(onTrino().executeQuery("SELECT * FROM " + tableName).rows())
                    .hasSize(4)
                    .containsExactlyInAnyOrderElementsOf(onTrino().executeQuery("SELECT * FROM tpch.tiny.region WHERE regionkey <> 0").rows());
            assertThat(onTrino().executeQuery("SELECT count(1) FROM \"" + tableName + "$files\" WHERE content = 1 AND file_format = 'PUFFIN'"))
                    .containsOnly(row(1L));
        }
        finally {
            onTrino().executeQuery("DROP TABLE " + tableName);
        }
    }

    @Test(groups = {OBJECTSTORE, PROFILE_SPECIFIC_TESTS})
    public void testIcebergPartitionStats()
    {
        onTrino().executeQuery("USE minio.test");
        String tableName = "test_partition_stats" + randomNameSuffix();
        onTrino().executeQuery("SET SESSION minio.partition_statistics_collect_on_write = true");
        onTrino().executeQuery("CREATE TABLE " + tableName + " WITH (type = 'ICEBERG', partitioning = ARRAY['regionkey']) AS SELECT * FROM tpch.tiny.nation");
        try {
            assertThat(onTrino().executeQuery("SELECT * FROM " + tableName).rows())
                    .containsExactlyInAnyOrderElementsOf(onTrino().executeQuery("SELECT * FROM tpch.tiny.nation").rows());

            assertThat(onTrino().executeQuery("SHOW STATS FOR " + tableName))
                    .containsOnly(
                            row("nationkey", null, 25.0, 0.0, null, null, null),
                            row("name", null, 25.0, 0.0, null, null, null),
                            row("regionkey", null, 5.0, 0.03, null, null, null),
                            row("comment", null, 25.0, 0.0, null, null, null),
                            row(null, null, null, null, 25.0, null, null));
        }
        finally {
            onTrino().executeQuery("DROP TABLE " + tableName);
        }
    }
}
