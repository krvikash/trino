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

import io.trino.tempto.ProductTest;
import org.testng.annotations.Test;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tests.product.TestGroups.OBJECTSTORE;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static org.assertj.core.api.Assertions.assertThat;

public class TestObjectStore
        extends ProductTest
{
    private static final String S3_BUCKET_NAME = "test-bucket";

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
}
