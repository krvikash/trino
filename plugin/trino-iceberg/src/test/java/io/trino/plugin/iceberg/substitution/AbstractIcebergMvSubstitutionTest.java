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
import io.trino.plugin.iceberg.TestingIcebergPlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.sql.tree.ExplainType;
import io.trino.testing.QueryRunner;
import io.trino.testing.substitution.AbstractMvSubstitutionFullTest;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.Map;

import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.plugin.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class AbstractIcebergMvSubstitutionTest
        extends AbstractMvSubstitutionFullTest
{
    @Override
    protected String partitionedByPropertyName()
    {
        return "partitioning";
    }

    protected abstract QueryRunner createSourceQueryRunner(Session defaultSession, CatalogSchemaName sourceSchema)
            throws Exception;

    @Override
    protected CatalogSchemaName mvSchema()
    {
        return new CatalogSchemaName(ICEBERG_CATALOG, "tpch");
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        CatalogSchemaName sourceSchema = sourceSchema();
        QueryRunner queryRunner = createSourceQueryRunner(testSessionBuilder()
                        .setCatalog(sourceSchema.getCatalogName())
                        .setSchema(sourceSchema.getSchemaName())
                        .build(),
                sourceSchema);
        try {
            if (addIcebergConnector()) {
                Path baseDataDir = queryRunner.getCoordinator().getBaseDataDir();
                queryRunner.installPlugin(new TestingIcebergPlugin(baseDataDir));
                queryRunner.createCatalog(ICEBERG_CATALOG, "iceberg", Map.of(
                        "iceberg.catalog.type", "TESTING_FILE_METASTORE",
                        "hive.metastore.catalog.dir", "local:///iceberg-catalog",
                        "iceberg.hive-catalog-name", "hive",
                        // v3 + variant so MV storage can hold a Trino json column materialized from a
                        // JSON source column (exercised by testScanWithSubFieldProjectionOverMv).
                        "iceberg.format-version", "3",
                        "iceberg.legacy-variant-type-mapping", "JSON"));
                queryRunner.execute("CREATE SCHEMA %s.tpch".formatted(ICEBERG_CATALOG));
            }
            if (addTpchConnector()) {
                queryRunner.installPlugin(new TpchPlugin());
                queryRunner.createCatalog("tpch", "tpch");
            }

            if (createSourceSchema()) {
                queryRunner.execute("CREATE SCHEMA " + sourceSchema);
            }
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
        return queryRunner;
    }

    protected boolean addIcebergConnector()
    {
        return true;
    }

    protected boolean addTpchConnector()
    {
        return true;
    }

    protected boolean createSourceSchema()
    {
        return true;
    }

    @Test
    public void testExplainShowsSubstitutedMaterializedView()
    {
        CatalogSchemaTableName mvName = mvName("mv_explain_substituted_");
        try {
            assertUpdate("CREATE MATERIALIZED VIEW " + mvName + " WITH (substitution_enabled = true) AS SELECT * FROM " + ordersTable);
            getQueryRunner().execute("REFRESH MATERIALIZED VIEW " + mvName);

            Session session = sessionWithSubstitution();
            String substitutedExplain = getExplainPlan(session, "SELECT * FROM " + ordersTable, ExplainType.Type.DISTRIBUTED);
            assertThat(substitutedExplain)
                    .contains("Substituted materialized views:")
                    .contains(mvName + " (refreshed ")
                    .contains("replaced plan:")
                    .contains("TableScan");

            String nonSubstitutedExplain = getExplainPlan(session, "SELECT * FROM " + lineitemTable, ExplainType.Type.DISTRIBUTED);
            assertThat(nonSubstitutedExplain).doesNotContain("Substituted materialized views");
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + mvName);
        }
    }

    @Test
    public void testExplainAnalyzeShowsSubstitutedMaterializedView()
    {
        // EXPLAIN ANALYZE renders substitutions from QueryStats rather than replanning via QueryExplainer.
        CatalogSchemaTableName mvName = mvName("mv_explain_analyze_substituted_");
        try {
            assertUpdate("CREATE MATERIALIZED VIEW " + mvName + " WITH (substitution_enabled = true) AS SELECT * FROM " + ordersTable);
            getQueryRunner().execute("REFRESH MATERIALIZED VIEW " + mvName);

            Session session = sessionWithSubstitution();
            String explainAnalyze = (String) computeActual(session, "EXPLAIN ANALYZE SELECT * FROM " + ordersTable).getOnlyValue();
            assertThat(explainAnalyze)
                    .contains("Substituted materialized views:")
                    .contains(mvName + " (refreshed ")
                    .contains("replaced plan:")
                    .contains("TableScan");
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + mvName);
        }
    }

    @Test
    public void testExplainShowsSubstitutedMaterializedViewWithFilterAndProject()
    {
        CatalogSchemaTableName mvName = mvName("mv_explain_filter_project_");
        try {
            assertUpdate("CREATE MATERIALIZED VIEW " + mvName + " WITH (substitution_enabled = true) AS SELECT orderkey, custkey, orderstatus, totalprice FROM " + ordersTable);
            getQueryRunner().execute("REFRESH MATERIALIZED VIEW " + mvName);

            Session session = sessionWithSubstitution();
            String explain = getExplainPlan(session, "SELECT orderkey, totalprice FROM " + ordersTable + " WHERE orderstatus = 'F'", ExplainType.Type.DISTRIBUTED);
            assertThat(explain)
                    .contains(mvName + " (refreshed ")
                    .contains("replaced plan:")
                    .contains("TableScan");
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + mvName);
        }
    }

    @Test
    public void testExplainByteIdenticalWhenNoSubstitutionOccurs()
    {
        // A candidate MV that is rejected during matching (a near-miss, not just the absence of any
        // MV) must still leave the rendered EXPLAIN text unchanged: only SUBSTITUTED outcomes render
        // the header, so the text must be identical whether the session-level feature is on or off.
        CatalogSchemaTableName mvName = mvName("mv_explain_near_miss_");
        try {
            assertUpdate("CREATE MATERIALIZED VIEW " + mvName + " WITH (substitution_enabled = true) AS SELECT orderkey, custkey FROM " + ordersTable);
            getQueryRunner().execute("REFRESH MATERIALIZED VIEW " + mvName);

            String query = "SELECT orderkey, totalprice FROM " + ordersTable;
            assertNotSubstituted(sessionWithSubstitution(), query, ordersTable);

            String withFeatureOn = getExplainPlan(sessionWithSubstitution(), query, ExplainType.Type.DISTRIBUTED);
            String withFeatureOff = getExplainPlan(sessionWithoutSubstitution(), query, ExplainType.Type.DISTRIBUTED);
            assertThat(withFeatureOn)
                    .doesNotContain("Substituted materialized views")
                    .isEqualTo(withFeatureOff);
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + mvName);
        }
    }

    @Test
    public void testExplainSelfJoinSubstitutionRendersOneEntryPerDistinctReplacedPlan()
    {
        // A self-join substitutes each occurrence of the base table independently, producing two
        // SUBSTITUTED rows for the same (materialized view, base table, refresh time) triple. The block
        // collapses two such rows into a single entry only when their replaced subtrees render
        // identically; every occurrence of the base table must still be represented by some entry.
        CatalogSchemaTableName mvName = mvName("mv_explain_self_join_");
        try {
            assertUpdate("CREATE MATERIALIZED VIEW " + mvName + " WITH (substitution_enabled = true) AS SELECT * FROM " + ordersTable);
            getQueryRunner().execute("REFRESH MATERIALIZED VIEW " + mvName);

            String query = "SELECT a.orderkey FROM " + ordersTable + " a JOIN " + ordersTable + " b ON a.orderkey = b.orderkey";
            String explain = getExplainPlan(sessionWithSubstitution(), query, ExplainType.Type.DISTRIBUTED);
            long headerLines = explain.lines().filter(line -> line.contains(mvName + " (refreshed ")).count();
            long replacedPlanBlocks = explain.lines().filter(line -> line.contains("replaced plan:")).count();
            assertThat(headerLines).isBetween(1L, 2L);
            assertThat(replacedPlanBlocks).isEqualTo(headerLines);
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + mvName);
        }
    }
}
