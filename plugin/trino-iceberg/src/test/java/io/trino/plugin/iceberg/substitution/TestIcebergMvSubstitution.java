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
package io.trino.plugin.iceberg.substitution;

import io.trino.Session;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.sql.tree.ExplainType;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestMaterializedView;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@Execution(SAME_THREAD)
public class TestIcebergMvSubstitution
        extends AbstractIcebergOnIcebergMvSubstitutionTest
{
    // Aggregation arguments are bigint, so results compare exactly, and finished is a bare boolean column, so a FILTER
    // on it needs no projection under the aggregation
    private final CatalogSchemaTableName aggregationTable = sourceTable("agg_orders_" + randomNameSuffix());

    @Override
    protected QueryRunner createSourceQueryRunner(Session defaultSession, CatalogSchemaName sourceSchema)
            throws Exception
    {
        QueryRunner queryRunner = DistributedQueryRunner.builder(defaultSession)
                .addExtraProperty("materialized-view-substitution.support.enabled", "true")
                .addExtraProperty("optimizer.reuse-common-subqueries", "true")
                .build();

        return queryRunner;
    }

    @BeforeAll
    public void createAggregationTable()
    {
        getQueryRunner().execute("CREATE TABLE " + aggregationTable + " AS SELECT " +
                "orderkey, " +
                "custkey, " +
                "CAST(orderstatus AS VARCHAR) AS orderstatus, " +
                "orderstatus = 'F' AS finished " +
                "FROM tpch.tiny.orders WHERE orderkey < 1000");
    }

    @AfterAll
    public void dropAggregationTable()
    {
        dropSourceTable(aggregationTable);
    }

    @Test
    public void testAggregationWithSameAndSubsetOfAggregations()
    {
        try (TestMaterializedView materializedView = createSubstitutionMv(
                "mv_agg_",
                "SELECT custkey, sum(orderkey) AS total, count(*) AS row_count, max(orderkey) AS maximum, avg(orderkey) AS average FROM " + aggregationTable + " GROUP BY custkey")) {
            assertSubstituted("SELECT avg(orderkey), max(orderkey), custkey, count(*), sum(orderkey) FROM " + aggregationTable + " GROUP BY custkey", aggregationTable, materializedView.mvName());
            assertSubstituted("SELECT custkey, sum(orderkey) FROM " + aggregationTable + " GROUP BY custkey", aggregationTable, materializedView.mvName());
            assertSubstituted("SELECT max(orderkey), sum(orderkey), custkey FROM " + aggregationTable + " GROUP BY custkey", aggregationTable, materializedView.mvName());
        }
    }

    @Test
    public void testAggregationNotSubstitutedWhenComputationDiffers()
    {
        try (TestMaterializedView _ = createSubstitutionMv("mv_agg_mismatch_", "SELECT custkey, sum(orderkey) AS total FROM " + aggregationTable + " GROUP BY custkey")) {
            Session session = sessionWithSubstitution();
            assertNotSubstituted(session, "SELECT custkey, min(orderkey) FROM " + aggregationTable + " GROUP BY custkey", aggregationTable);
            assertNotSubstituted(session, "SELECT custkey, orderstatus, sum(orderkey) FROM " + aggregationTable + " GROUP BY custkey, orderstatus", aggregationTable);
            assertNotSubstituted(session, "SELECT sum(orderkey) FROM " + aggregationTable, aggregationTable);
        }
    }

    @Test
    public void testAggregationSubstitutedInEveryUnionBranch()
    {
        // The second branch's symbols get engine-generated suffixes, so they differ from the MV's
        try (TestMaterializedView materializedView = createSubstitutionMv(
                "mv_agg_union_",
                "SELECT custkey AS customer, sum(orderkey) AS total, max(orderkey) AS maximum FROM " + aggregationTable + " GROUP BY custkey")) {
            assertSubstituted(
                    "SELECT custkey, sum(orderkey) FROM " + aggregationTable + " GROUP BY custkey " +
                            "UNION ALL SELECT custkey, max(orderkey) FROM " + aggregationTable + " GROUP BY custkey",
                    aggregationTable,
                    materializedView.mvName());
        }
    }

    @Test
    public void testStackedAggregations()
    {
        // The outer aggregation reads every inner output: dropping one leaves a pruning projection between the two
        String inner = "SELECT custkey k, orderstatus status, sum(orderkey) s FROM " + aggregationTable + " GROUP BY custkey, orderstatus";
        try (TestMaterializedView materializedView = createSubstitutionMv("mv_agg_stacked_", "SELECT k, max(s) AS maximum, count(status) AS statuses FROM (" + inner + ") GROUP BY k")) {
            assertSubstituted("SELECT k, max(s), count(status) FROM (" + inner + ") GROUP BY k", aggregationTable, materializedView.mvName());
        }
    }

    @Test
    public void testSingleDistinctAggregation()
    {
        try (TestMaterializedView materializedView = createSubstitutionMv("mv_agg_distinct_", "SELECT custkey, count(DISTINCT orderstatus) AS statuses FROM " + aggregationTable + " GROUP BY custkey")) {
            assertSubstituted("SELECT custkey, count(DISTINCT orderstatus) FROM " + aggregationTable + " GROUP BY custkey", aggregationTable, materializedView.mvName());
        }
    }

    @Test
    public void testMultipleDistinctAggregations()
    {
        try (TestMaterializedView materializedView = createSubstitutionMv(
                "mv_agg_distincts_",
                "SELECT custkey, count(DISTINCT orderstatus) AS statuses, count(DISTINCT orderkey) AS orders, sum(orderkey) AS total FROM " + aggregationTable + " GROUP BY custkey")) {
            assertSubstituted("SELECT custkey, count(DISTINCT orderstatus), count(DISTINCT orderkey) FROM " + aggregationTable + " GROUP BY custkey", aggregationTable, materializedView.mvName());
            assertSubstituted("SELECT custkey, count(DISTINCT orderkey), sum(orderkey) FROM " + aggregationTable + " GROUP BY custkey", aggregationTable, materializedView.mvName());
        }
    }

    @Test
    public void testFilteredAggregations()
    {
        try (TestMaterializedView materializedView = createSubstitutionMv(
                "mv_agg_filter_",
                "SELECT custkey, count(*) FILTER (WHERE finished) AS finished_orders, sum(orderkey) FILTER (WHERE finished) AS finished_total FROM " + aggregationTable + " GROUP BY custkey")) {
            assertSubstituted(
                    "SELECT custkey, count(*) FILTER (WHERE finished), sum(orderkey) FILTER (WHERE finished) FROM " + aggregationTable + " GROUP BY custkey",
                    aggregationTable,
                    materializedView.mvName());
            assertNotSubstituted(sessionWithSubstitution(), "SELECT custkey, count(*), sum(orderkey) FILTER (WHERE finished) FROM " + aggregationTable + " GROUP BY custkey", aggregationTable);
        }
    }

    @Test
    public void testOrderedAggregation()
    {
        try (TestMaterializedView materializedView = createSubstitutionMv("mv_agg_ordered_", "SELECT custkey, array_agg(orderkey ORDER BY orderkey DESC) AS keys FROM " + aggregationTable + " GROUP BY custkey")) {
            assertSubstituted("SELECT custkey, array_agg(orderkey ORDER BY orderkey DESC) FROM " + aggregationTable + " GROUP BY custkey", aggregationTable, materializedView.mvName());
            assertNotSubstituted(sessionWithSubstitution(), "SELECT custkey, array_agg(orderkey ORDER BY orderkey) FROM " + aggregationTable + " GROUP BY custkey", aggregationTable);
        }
    }

    @Test
    public void testGlobalAggregation()
    {
        try (TestMaterializedView materializedView = createSubstitutionMv("mv_agg_global_", "SELECT count(*) AS row_count FROM " + aggregationTable)) {
            assertSubstituted("SELECT count(*) FROM " + aggregationTable, aggregationTable, materializedView.mvName());
        }
    }

    @Test
    public void testExplainShowsReplacedAggregation()
    {
        try (TestMaterializedView materializedView = createSubstitutionMv("mv_agg_explain_", "SELECT custkey, sum(orderkey) AS total FROM " + aggregationTable + " GROUP BY custkey")) {
            String explain = getExplainPlan(sessionWithSubstitution(), "SELECT custkey, sum(orderkey) FROM " + aggregationTable + " GROUP BY custkey", ExplainType.Type.DISTRIBUTED);
            assertThat(explain).contains(materializedView.mvName() + " (refreshed ");
            assertThat(explain.substring(explain.indexOf("replaced plan:"))).contains("Aggregate");
        }
    }
}
