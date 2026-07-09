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

import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.tests.tpch.TpchQueryRunnerBuilder;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

final class TestSingleNodeSystemTables
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return TpchQueryRunnerBuilder.builder()
                .setWorkerCount(1)
                .addExtraProperty("node-scheduler.include-coordinator", "false")
                .addExtraProperty("experimental.force-single-node-query", "true")
                .build();
    }

    @Test
    void testCoordinatorOnlySystemTable()
    {
        // system.runtime.nodes has SINGLE_COORDINATOR distribution and must run on the coordinator
        assertThat(query("SELECT count(*) FROM system.runtime.nodes"))
                .matches("VALUES BIGINT '2'");
    }

    @Test
    void testJoinCoordinatorOnlySystemTableWithRegularTable()
    {
        // SNEM is disabled for this query because the system table doesn't support single-node execution,
        // so the distributed plan correctly routes each scan to the appropriate node.
        assertThat(query(
                """
                SELECT n.name, c.catalog_name
                FROM tpch.tiny.nation n
                JOIN system.metadata.catalogs c ON c.catalog_name = 'tpch'
                WHERE n.name = 'FRANCE'
                """))
                .skippingTypesCheck()
                .matches("VALUES (VARCHAR 'FRANCE', VARCHAR 'tpch')");
    }

    @Test
    void testAllNodesSystemTable()
    {
        // system.runtime.tasks has ALL_NODES distribution — SNEM is disabled for system tables,
        // so each split runs on its own node and no duplicates are produced.
        assertThat(query("SELECT count(*) = count(DISTINCT task_id) FROM system.runtime.tasks"))
                .matches("VALUES true");
    }

    @Test
    void testInformationSchemaTable()
    {
        assertThat(query("SELECT count(*) FROM tpch.information_schema.tables WHERE table_schema = 'tiny'"))
                .matches("VALUES BIGINT '8'");
    }
}
