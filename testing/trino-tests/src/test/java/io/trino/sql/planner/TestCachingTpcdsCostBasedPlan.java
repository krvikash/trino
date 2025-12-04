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

package io.trino.sql.planner;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.cache.CacheConfig;
import io.trino.testing.PlanTester;
import io.trino.tpcds.Table;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.trino.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static io.trino.testing.PlanTesterBuilder.planTesterBuilder;
import static io.trino.testing.TestingSession.testSessionBuilder;

/**
 * This class tests cost-based optimization rules. It contains unmodified TPC-DS queries.
 * This class is using Iceberg connector un-partitioned TPC-DS tables.
 */
public class TestCachingTpcdsCostBasedPlan
        extends BaseCostBasedPlanTest
{
    protected TestCachingTpcdsCostBasedPlan()
    {
        super("tpcds_sf1000_parquet_part", true);
    }

    @Override
    protected PlanTester createPlanTester()
    {
        Session.SessionBuilder sessionBuilder = testSessionBuilder()
                .setCatalog(CATALOG_NAME)
                .setSchema("tpcds_sf1000_parquet_part")
                // Reducing ARM and x86 floating point arithmetic differences, mostly visible at PlanNodeStatsEstimateMath::estimateCorrelatedConjunctionRowCount
                .setSystemProperty("filter_conjunction_independence_factor", "0.750000001")
                .setSystemProperty("task_concurrency", "1") // these tests don't handle exchanges from local parallel
                .setSystemProperty(JOIN_REORDERING_STRATEGY, OptimizerConfig.JoinReorderingStrategy.AUTOMATIC.name())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, OptimizerConfig.JoinDistributionType.AUTOMATIC.name());
        PlanTester planTester = planTesterBuilder(sessionBuilder.build())
                .withNodeCountForStats(8)
                .withCacheConfig(new CacheConfig()
                        .setEnabled(true)
                        .setCacheCommonSubqueriesEnabled(true))
                .build();
        planTester.createCatalog(
                CATALOG_NAME,
                planTestSetup.createConnectorFactory(),
                ImmutableMap.of());
        return planTester;
    }

    @Override
    protected List<String> getTableNames()
    {
        return Table.getBaseTables().stream()
                .filter(table -> table != Table.DBGEN_VERSION)
                .map(Table::getName)
                .collect(toImmutableList());
    }

    @Override
    protected String getQueryPlanResourcePath(String queryResourcePath)
    {
        Path queryPath = Paths.get(queryResourcePath);
        Path directory = queryPath.getParent();
        directory = directory.resolve("iceberg").resolve("cache");
        String planResourceName = queryPath.getFileName().toString().replaceAll("\\.sql$", ".plan.txt");
        return directory.resolve(planResourceName).toString();
    }

    @Override
    protected String getTableResourceDirectory()
    {
        return "iceberg/tpcds/sf1000/partitioned/";
    }

    @Override
    protected String getTableTargetDirectory()
    {
        return "iceberg-tpcds-sf1000-parquet-part/";
    }

    @Override
    protected List<String> getQueryResourcePaths()
    {
        return TPCDS_SQL_FILES;
    }

    static void main()
    {
        new TestCachingTpcdsCostBasedPlan().generate();
    }
}
