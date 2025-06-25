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

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorPlugin;
import io.trino.plugin.memory.MemoryQueryRunner;
import io.trino.testing.AbstractDistributedEngineOnlyQueries;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.SystemSessionProperties.ENABLE_DYNAMIC_FILTERING;
import static io.trino.sql.planner.OptimizerConfig.JoinDistributionType.BROADCAST;

public class TestDistributedEngineOnlyQueriesWithCteReuse
        extends AbstractDistributedEngineOnlyQueries
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Map<String, String> exchangeManagerProperties = ImmutableMap.<String, String>builder()
                .put("exchange.base-directories", System.getProperty("java.io.tmpdir") + "/trino-local-file-system-exchange-manager")
                .buildOrThrow();

        QueryRunner queryRunner = MemoryQueryRunner.builder()
                .addExtraProperty("optimizer.reuse-common-subqueries", "true")
                .setInitialTables(REQUIRED_TPCH_TABLES)
                .setAdditionalSetup(runner -> {
                    runner.installPlugin(new io.trino.plugin.exchange.filesystem.FileSystemExchangePlugin());
                    runner.loadExchangeManager("filesystem", exchangeManagerProperties);
                })
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

    @Override
    @Test
    @Disabled
    public void testLogicalExplainTextFormat()
    {
        // EXPLAIN TYPE LOGICAL is not supported in new IR
    }

    @Override
    @Test
    @Disabled
    public void testExplainExecute()
    {
        // EXPLAIN TYPE LOGICAL is not supported in new IR
    }

    @Override
    @Test
    @Disabled
    public void testLogicalExplainGraphvizFormat()
    {
        // EXPLAIN TYPE LOGICAL is not supported in new IR
    }

    @Override
    @Test
    @Disabled
    public void testExplainExecuteWithUsing()
    {
        // EXPLAIN TYPE LOGICAL is not supported in new IR
    }

    @Override
    @Test
    @Disabled
    public void testIoExplain()
    {
        // EXPLAIN TYPE IO is not supported in new IR
    }

    @Override
    @Test
    @Disabled
    public void testLogicalExplain()
    {
        // EXPLAIN TYPE LOGICAL is not supported in new IR
    }

    @Override
    @Test
    public void testExplainAnalyze()
    {
        assertExplainAnalyze(
                noJoinReordering(BROADCAST),
                "EXPLAIN ANALYZE SELECT * FROM (SELECT nationkey, regionkey FROM nation GROUP BY nationkey, regionkey) a, nation b WHERE a.regionkey = b.regionkey",
                "Trino version: .*");
        assertExplainAnalyze(
                "EXPLAIN ANALYZE SELECT * FROM nation a, nation b WHERE a.nationkey = b.nationkey",
                "Left \\(probe\\) Input avg\\.: .* rows, Input std\\.dev\\.: .*",
                "Right \\(build\\) Input avg\\.: .* rows, Input std\\.dev\\.: .*");
        assertExplainAnalyze(
                Session.builder(getSession())
                        .setSystemProperty(ENABLE_DYNAMIC_FILTERING, "false")
                        .build(),
                "EXPLAIN ANALYZE SELECT * FROM nation a, nation b WHERE a.nationkey = b.nationkey",
                "Left \\(probe\\) Input avg\\.: .* rows, Input std\\.dev\\.: .*",
                "Right \\(build\\) Input avg\\.: .* rows, Input std\\.dev\\.: .*");

        // The last test case is removed: in this case, the plan is fragmented from the new IR. The estimates are not available.
    }
}
