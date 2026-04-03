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
package io.trino.tests;

import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorPlugin;
import io.trino.plugin.memory.MemoryQueryRunner;
import io.trino.testing.AbstractDistributedEngineOnlyQueries;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDistributedGpuEngineOnlyQueries
        extends AbstractDistributedEngineOnlyQueries
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = MemoryQueryRunner.builder()
                .setInitialTables(REQUIRED_TPCH_TABLES)
                .addExtraProperty("gpu-acceleration.enabled", "true")
                .build();
        try {
            queryRunner.getCoordinator().getSessionPropertyManager().addSystemSessionProperties(TEST_SYSTEM_PROPERTIES);
            queryRunner.installPlugin(new MockConnectorPlugin(MockConnectorFactory.builder()
                    .withSessionProperties(TEST_CATALOG_PROPERTIES)
                    .build()));
            queryRunner.createCatalog(TESTING_CATALOG, "mock");
        }
        catch (RuntimeException e) {
            throw closeAllSuppress(e, queryRunner);
        }
        return queryRunner;
    }

    @Test
    public void testLikeFilter()
    {
        assertThat(query(
                """
                SELECT s
                FROM (SELECT CAST(i AS varchar) AS s FROM (UNNEST(sequence(0, 1000, 13))) t(i))
                WHERE s LIKE '%6%7%'
                """))
                .matches("VALUES VARCHAR '637', '676', '767'");

        assertThat(query(
                """
                SELECT s
                -- Use rand() to prevent Projection from being inlined in Filter, so that LIKE operates directly on a varchar input column
                FROM (SELECT IF(rand()<42, CAST(i AS varchar)) AS s FROM (UNNEST(sequence(0, 1000, 13))) t(i))
                WHERE s LIKE '%6%7%'
                """))
                .matches("VALUES VARCHAR '637', '676', '767'");

        assertThat(query("SELECT name FROM nation WHERE comment LIKE '%a%a___a%'"))
                .matches("VALUES CAST('BRAZIL' AS varchar(25)), 'CANADA', 'JORDAN', 'MOROCCO', 'ROMANIA'");
    }
}
