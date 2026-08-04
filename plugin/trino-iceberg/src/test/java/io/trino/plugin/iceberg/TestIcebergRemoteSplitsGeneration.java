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

import io.trino.Session;
import io.trino.operator.OperatorStats;
import io.trino.plugin.base.metrics.LongCount;
import io.trino.spi.QueryId;
import io.trino.spi.metrics.Metric;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.QueryRunner.MaterializedResultWithPlan;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.NoSuchElementException;

import static com.google.common.collect.MoreCollectors.onlyElement;
import static io.trino.plugin.iceberg.IcebergSessionProperties.DYNAMIC_FILTERING_WAIT_TIMEOUT;
import static org.assertj.core.api.Assertions.assertThat;

final class TestIcebergRemoteSplitsGeneration
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .addIcebergProperty("iceberg.remote-splits-generation.enabled", "true")
                .addIcebergProperty("iceberg.remote-splits-generation.manifests-per-thread", "0")
                .build();
    }

    @Test
    void testRemoteSplitsGenerationIsUsed()
    {
        try (TestTable table = newTrinoTable("test_remote_splits_generation_used", "(id int)")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES 1, 2, 3", 3);
            MaterializedResultWithPlan result = getDistributedQueryRunner().executeWithPlan(getSession(), "SELECT * FROM " + table.getName());

            // check the remote splits source's own metrics
            Map<String, Metric<?>> metrics = getOperatorStats(result.queryId()).getConnectorMetrics().getMetrics();
            assertThat(((LongCount) metrics.get("remoteSplitsSource.taskCreateAttempts")).getTotal()).isGreaterThanOrEqualTo(1);
            assertThat(((LongCount) metrics.get("remoteSplitsSource.batchesFetched")).getTotal()).isGreaterThanOrEqualTo(1);
        }
    }

    @Test
    void testRemoteSplitsGenerationCanBeDisabled()
    {
        try (TestTable table = newTrinoTable("test_remote_splits_generation_disabled", "(id int)")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES 1, 2, 3", 3);
            Session session = Session.builder(getSession())
                    .setCatalogSessionProperty("iceberg", "remote_splits_generation_enabled", "false")
                    .build();
            MaterializedResultWithPlan result = getDistributedQueryRunner().executeWithPlan(session, "SELECT * FROM " + table.getName());
            assertThat(getOperatorStats(result.queryId()).getConnectorMetrics().getMetrics().keySet())
                    .noneMatch(key -> key.startsWith("remoteSplitsSource."));
        }
    }

    @Test
    void testDynamicFilteringOverRemoteSplitsGeneration()
    {
        try (TestTable dataTable = newTrinoTable("test_remote_split_df_data", "(id int, value varchar)");
                TestTable filterTable = newTrinoTable("test_remote_split_df_filter", "(filter_id int)")) {
            assertUpdate("INSERT INTO " + dataTable.getName() + " VALUES (1, 'a'), (2, 'b')", 2);
            assertUpdate("INSERT INTO " + dataTable.getName() + " VALUES (3, 'c'), (4, 'd')", 2);
            assertUpdate("INSERT INTO " + filterTable.getName() + " VALUES (1), (3)", 2);

            Session session = Session.builder(getSession())
                    .setCatalogSessionProperty("iceberg", DYNAMIC_FILTERING_WAIT_TIMEOUT, "1s")
                    .build();
            String sql = "SELECT d.id, d.value FROM %s d JOIN %s f ON d.id = f.filter_id ORDER BY d.id"
                    .formatted(dataTable.getName(), filterTable.getName());
            assertQuery(session, sql, "VALUES (1, 'a'), (3, 'c')");
        }
    }

    private OperatorStats getOperatorStats(QueryId queryId)
    {
        try {
            return getDistributedQueryRunner().getCoordinator()
                    .getQueryManager()
                    .getFullQueryInfo(queryId)
                    .getQueryStats()
                    .getOperatorSummaries()
                    .stream()
                    .filter(summary -> summary.getOperatorType().startsWith("TableScan") || summary.getOperatorType().startsWith("Scan"))
                    .collect(onlyElement());
        }
        catch (NoSuchElementException e) {
            throw new RuntimeException("Couldn't find operator summary, probably due to query statistic collection error", e);
        }
    }
}
