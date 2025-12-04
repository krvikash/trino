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
import io.trino.spi.catalog.CatalogName;
import io.trino.testing.PlanTester;
import io.trino.tpch.TpchTable;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.SystemSessionProperties.IGNORE_STATS_CALCULATOR_FAILURES;
import static io.trino.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.trino.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static io.trino.SystemSessionProperties.REUSE_COMMON_SUBQUERIES;
import static io.trino.execution.querystats.PlanOptimizersStatsCollector.createPlanOptimizersStatsCollector;
import static io.trino.execution.warnings.WarningCollector.NOOP;
import static io.trino.sql.newir.FormatOptions.TESTING_PRINT_OPTIONS;
import static io.trino.sql.planner.LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED;
import static io.trino.testing.TestingSession.testSessionBuilder;

/**
 * This class tests the full planning and optimization result with CTE reuse enabled.
 * It uses Iceberg connector with unpartitioned TPC-H tables.
 * <p>
 * The result is presented in the form of the new IR assembly.
 * In case where a query cannot be represented in the new IR, or CTE reuse is ineffective,
 * the expected result is a text file containing this information instead of the plan.
 */
public class TestCteReuseTpchPlan
        extends BaseCostBasedPlanTest
{
    private static final String SCHEMA_NAME = "tpch_sf1000_parquet";

    protected TestCteReuseTpchPlan()
    {
        super(SCHEMA_NAME, false);
    }

    @Override
    protected PlanTester createPlanTester()
    {
        Session.SessionBuilder sessionBuilder = testSessionBuilder()
                .setCatalog(CATALOG_NAME)
                .setSchema(SCHEMA_NAME)
                // Reducing ARM and x86 floating point arithmetic differences, mostly visible at PlanNodeStatsEstimateMath::estimateCorrelatedConjunctionRowCount
                .setSystemProperty("filter_conjunction_independence_factor", "0.750000001")
                .setSystemProperty("task_concurrency", "1") // these tests don't handle exchanges from local parallel
                .setSystemProperty(JOIN_REORDERING_STRATEGY, OptimizerConfig.JoinReorderingStrategy.AUTOMATIC.name())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, OptimizerConfig.JoinDistributionType.AUTOMATIC.name())
                .setSystemProperty(IGNORE_STATS_CALCULATOR_FAILURES, "false")
                .setSystemProperty(REUSE_COMMON_SUBQUERIES, "true");
        PlanTester planTester = PlanTester.create(sessionBuilder.build(), 8);
        planTester.createCatalog(
                CATALOG_NAME,
                planTestSetup.createConnectorFactory(),
                ImmutableMap.of());
        return planTester;
    }

    @Override
    protected List<String> getTableNames()
    {
        return TpchTable.getTables().stream()
                .map(TpchTable::getTableName)
                .collect(toImmutableList());
    }

    @Override
    protected String getTableResourceDirectory()
    {
        return "iceberg/tpch/sf1000/unpartitioned/";
    }

    @Override
    protected String getTableTargetDirectory()
    {
        return "iceberg-tpch-sf1000-parquet/";
    }

    @Override
    protected List<String> getQueryResourcePaths()
    {
        return TPCH_SQL_FILES;
    }

    @Override
    protected String getQueryPlanResourcePath(String queryResourcePath)
    {
        Path queryPath = Paths.get(queryResourcePath);
        String connectorName = getPlanTester().getCatalogManager().getCatalog(new CatalogName(CATALOG_NAME)).orElseThrow().getConnectorName().toString();
        Path directory = queryPath.getParent();
        directory = directory.resolve(connectorName);
        directory = directory.resolve("cte_reuse");
        String planResourceName = queryPath.getFileName().toString().replaceAll("\\.sql$", ".plan.txt");
        return directory.resolve(planResourceName).toString();
    }

    @Override
    protected String generateQueryPlan(String query)
    {
        try {
            return getPlanTester().inTransaction(transactionSession -> {
                PlanTester planTester = getPlanTester();
                LogicalPlanner.PlanOptions planOptions = planTester.createPlanOptions(
                        transactionSession,
                        query,
                        planTester.getPlanOptimizers(false),
                        planTester.getAlternativeOptimizers(),
                        OPTIMIZED_AND_VALIDATED,
                        NOOP,
                        createPlanOptimizersStatsCollector(),
                        true);
                if (planOptions.newIrProgram().isEmpty()) {
                    return "This query cannot be represented in the new IR or CTE reuse is ineffective.";
                }
                return planOptions.newIrProgram().get().print(TESTING_PRINT_OPTIONS);
            });
        }
        catch (RuntimeException e) {
            throw new AssertionError("Planning failed for SQL: " + query, e);
        }
    }

    static void main()
    {
        new TestCteReuseTpchPlan().generate();
    }
}
