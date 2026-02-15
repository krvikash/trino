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

import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Base class for testing external modifications from another Trino cluster.
 * Simulates scenarios where multiple independent Trino deployments write to the same metastore.
 */
public abstract class BaseIcebergExternalWriteTest
        extends AbstractTestQueryFramework
{
    protected abstract Map<String, String> getIcebergProperties();

    protected abstract String createSchemaSql(String schemaName);

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createCluster();
    }

    @Test
    void testExternalWrite()
            throws Exception
    {
        DistributedQueryRunner firstCluster = getDistributedQueryRunner();

        String schemaName = "test_schema_external_write" + randomNameSuffix();
        firstCluster.execute(createSchemaSql(schemaName));
        String tableName = "test_external_write";
        firstCluster.execute("CREATE TABLE %s.%s (col BIGINT)".formatted(schemaName, tableName));

        try (DistributedQueryRunner secondCluster = createCluster()) {
            // Populate cache on both clusters
            firstCluster.execute("SELECT count(*) FROM %s.%s".formatted(schemaName, tableName));
            secondCluster.execute("SELECT count(*) FROM %s.%s".formatted(schemaName, tableName));

            // Insert from first cluster
            firstCluster.execute("INSERT INTO %s.%s VALUES (1)".formatted(schemaName, tableName));

            // Insert from second cluster (external system). If the second cluster has stale metadata, the following INSERT will fail.
            secondCluster.execute("INSERT INTO %s.%s VALUES (2)".formatted(schemaName, tableName));

            // First cluster has stale metadata - flush cache to see the external write
            firstCluster.execute("CALL system.flush_metadata_cache(schema_name => '" + schemaName + "', table_name => '" + tableName + "')");
            assertThat(getRowCount(firstCluster, schemaName, tableName))
                    .isEqualTo(2);

            assertThat(getRowCount(secondCluster, schemaName, tableName))
                    .isEqualTo(2);
        }
        finally {
            firstCluster.execute("DROP TABLE IF EXISTS %s.%s".formatted(schemaName, tableName));
            firstCluster.execute("DROP SCHEMA IF EXISTS " + schemaName);
        }
    }

    private static long getRowCount(DistributedQueryRunner secondCluster, String schemaName, String tableName)
    {
        return (long) secondCluster.execute("SELECT count(*) FROM %s.%s".formatted(schemaName, tableName))
                .getOnlyValue();
    }

    protected DistributedQueryRunner createCluster()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .setIcebergProperties(getIcebergProperties())
                .disableSchemaInitializer()
                .build();
    }
}
