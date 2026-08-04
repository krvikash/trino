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
package io.trino.plugin.objectstore;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.operator.OperatorStats;
import io.trino.plugin.base.metrics.LongCount;
import io.trino.plugin.iceberg.IcebergPlugin;
import io.trino.spi.QueryId;
import io.trino.spi.metrics.Metric;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.QueryRunner.MaterializedResultWithPlan;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.Map;
import java.util.NoSuchElementException;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.MoreCollectors.onlyElement;
import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.plugin.objectstore.StarburstObjectStoreConnectorFactory.STARBURST_OBJECTSTORE;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;

final class TestObjectStoreIcebergRemoteSplitsGeneration
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(
                        testSessionBuilder()
                                .setCatalog("iceberg")
                                .setSchema("tpch")
                                .build())
                .build();
        try {
            Path dataDir = queryRunner.getCoordinator().getBaseDataDir().resolve("iceberg");
            verify(dataDir.toFile().mkdirs());

            queryRunner.installPlugin(new IcebergPlugin());
            queryRunner.installPlugin(new ObjectStorePlugin());
            queryRunner.createCatalog("iceberg", STARBURST_OBJECTSTORE, ImmutableMap.<String, String>builder()
                    .put("great-lakes.table-type", TableType.ICEBERG.name())
                    .put("hive.metastore", "file")
                    .put("hive.metastore.catalog.dir", "local://" + dataDir)
                    .put("fs.local.enabled", "true")
                    .put("iceberg.remote-splits-generation.enabled", "true")
                    .put("iceberg.remote-splits-generation.manifests-per-thread", "0")
                    .buildOrThrow());
            queryRunner.execute("CREATE SCHEMA iceberg.tpch");
            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    @Test
    void testRemoteSplitsGenerationIsUsedThroughObjectStore()
    {
        try (TestTable table = newTrinoTable("test_remote_splits_generation_used", "(id int)")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES 1, 2, 3", 3);
            MaterializedResultWithPlan result = getDistributedQueryRunner().executeWithPlan(getSession(), "SELECT * FROM " + table.getName());

            // the remote splits source's own metrics prove the flag reached the Iceberg delegate
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
