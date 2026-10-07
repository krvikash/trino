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
import io.trino.execution.Input;
import io.trino.plugin.iceberg.TestingIcebergPlugin;
import io.trino.plugin.memory.MemoryPlugin;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.QueryRunner.MaterializedResultWithPlan;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.starburst.server.substitution.MaterializedViewSubstitutionSessionProperties.MATERIALIZED_VIEW_SUBSTITUTION_CANDIDATE_VALIDATION_ENABLED;
import static io.starburst.server.substitution.MaterializedViewSubstitutionSessionProperties.MATERIALIZED_VIEW_SUBSTITUTION_ENABLED;
import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.plugin.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

/**
 * Enabling {@code substitution_enabled} on a materialized view that could never be used as a substitution
 * target must fail the DDL rather than leave the property set but inert. The two rejected cases are a
 * definition query reading from a connector without substitution support, and a definition query whose plan
 * shape the materialization IR cannot represent.
 */
@Execution(SAME_THREAD)
public class TestIcebergMvSubstitutionCandidateValidation
        extends AbstractTestQueryFramework
{
    private static final String NOT_A_CANDIDATE = ".*cannot be enabled for substitution: its definition query is not supported by materialized view substitution";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = DistributedQueryRunner.builder(
                        testSessionBuilder()
                                .setCatalog(ICEBERG_CATALOG)
                                .setSchema("test_schema")
                                .build())
                .addExtraProperty("materialized-view-substitution.support.enabled", "true")
                .build();
        try {
            // A catalog with no ConnectorSubstitutionMetadata, so a scan over it has no identity to persist.
            // Deliberately not tpch: that connector supports substitution.
            queryRunner.installPlugin(new MemoryPlugin());
            queryRunner.createCatalog("memory", "memory", Map.of());

            Path baseDataDir = queryRunner.getCoordinator().getBaseDataDir();
            queryRunner.installPlugin(new TestingIcebergPlugin(baseDataDir));
            queryRunner.createCatalog(ICEBERG_CATALOG, "iceberg", Map.of(
                    "iceberg.catalog.type", "TESTING_FILE_METASTORE",
                    "hive.metastore.catalog.dir", "local:///iceberg-catalog",
                    "iceberg.hive-catalog-name", "hive"));
            queryRunner.execute("CREATE SCHEMA iceberg.test_schema");
            queryRunner.execute("CREATE TABLE base_table AS SELECT 1 AS id, 'a' AS name");
            queryRunner.execute("CREATE TABLE memory.default.unsupported_src AS SELECT BIGINT '1' AS id");
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
        return queryRunner;
    }

    @Test
    public void testCreateOverUnsupportedConnectorFails()
    {
        assertQueryFails(
                "CREATE MATERIALIZED VIEW mv_unsupported_connector WITH (substitution_enabled = true) AS SELECT id FROM memory.default.unsupported_src",
                NOT_A_CANDIDATE);
        assertQueryReturnsEmptyResult("SHOW TABLES LIKE 'mv_unsupported_connector'");
    }

    @Test
    public void testCreateWithUnsupportedPlanShapeFails()
    {
        // random() is deliberately chosen as the unsupported plan: unlike an aggregation or a plain projection,
        // it can never become supported, because the query has to evaluate it afresh while the materialized
        // view holds values frozen at refresh time.
        assertQueryFails(
                "CREATE MATERIALIZED VIEW mv_non_deterministic WITH (substitution_enabled = true) AS SELECT random() AS r FROM base_table",
                NOT_A_CANDIDATE);
        assertQueryReturnsEmptyResult("SHOW TABLES LIKE 'mv_non_deterministic'");
    }

    @Test
    public void testCreateSupportedDefinitionSucceeds()
    {
        assertUpdate("CREATE MATERIALIZED VIEW mv_supported WITH (substitution_enabled = true) AS SELECT id, name FROM base_table");
        assertUpdate("DROP MATERIALIZED VIEW mv_supported");
    }

    @Test
    public void testCreateSupportedAggregationSucceeds()
    {
        assertUpdate("CREATE MATERIALIZED VIEW mv_supported_aggregation WITH (substitution_enabled = true) AS SELECT name, max(id) AS max_id, count(*) AS row_count FROM base_table GROUP BY name");
        assertUpdate("DROP MATERIALIZED VIEW mv_supported_aggregation");
    }

    @Test
    public void testCreateWithUnsupportedAggregationFails()
    {
        // GROUPING SETS put a GroupId node under the aggregation
        assertQueryFails(
                "CREATE MATERIALIZED VIEW mv_grouping_sets WITH (substitution_enabled = true) AS SELECT name, id, count(*) AS row_count FROM base_table GROUP BY GROUPING SETS ((name), (id))",
                NOT_A_CANDIDATE);
        assertQueryReturnsEmptyResult("SHOW TABLES LIKE 'mv_grouping_sets'");

        // approx_distinct is not a supported aggregate function
        assertQueryFails(
                "CREATE MATERIALIZED VIEW mv_approx_distinct WITH (substitution_enabled = true) AS SELECT name, approx_distinct(id) AS ids FROM base_table GROUP BY name",
                NOT_A_CANDIDATE);
        assertQueryReturnsEmptyResult("SHOW TABLES LIKE 'mv_approx_distinct'");

        // the argument is computed by a projection under the aggregation
        assertQueryFails(
                "CREATE MATERIALIZED VIEW mv_argument_expression WITH (substitution_enabled = true) AS SELECT name, max(id * 2) AS doubled FROM base_table GROUP BY name",
                NOT_A_CANDIDATE);
        assertQueryReturnsEmptyResult("SHOW TABLES LIKE 'mv_argument_expression'");
    }

    @Test
    public void testCreateWithSubstitutionDisabledIsNotValidated()
    {
        assertUpdate("CREATE MATERIALIZED VIEW mv_disabled WITH (substitution_enabled = false) AS SELECT id FROM memory.default.unsupported_src");
        assertUpdate("DROP MATERIALIZED VIEW mv_disabled");
    }

    @Test
    public void testCandidateValidationCanBeDisabled()
    {
        // With validation off the pre-validation behaviour is restored: the property is accepted and the view
        // is simply never indexed, so neither the create nor the refresh fails.
        Session session = sessionWithoutCandidateValidation();
        assertUpdate(session, "CREATE MATERIALIZED VIEW mv_validation_disabled WITH (substitution_enabled = true) AS SELECT id FROM memory.default.unsupported_src");
        try {
            assertThat((String) computeScalar("SHOW CREATE MATERIALIZED VIEW mv_validation_disabled"))
                    .contains("substitution_enabled = true");
            assertUpdate(session, "REFRESH MATERIALIZED VIEW mv_validation_disabled", 1);
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW mv_validation_disabled");
        }
    }

    @Test
    public void testSetPropertiesEnablingSubstitutionWithValidationDisabled()
    {
        Session session = sessionWithoutCandidateValidation();
        assertUpdate("CREATE MATERIALIZED VIEW mv_alter_validation_disabled WITH (substitution_enabled = false) AS SELECT id FROM memory.default.unsupported_src");
        try {
            assertUpdate(session, "ALTER MATERIALIZED VIEW mv_alter_validation_disabled SET PROPERTIES substitution_enabled = true");
            assertThat((String) computeScalar("SHOW CREATE MATERIALIZED VIEW mv_alter_validation_disabled"))
                    .contains("substitution_enabled = true");
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW mv_alter_validation_disabled");
        }
    }

    @Test
    public void testCreateOrReplaceWithUnsupportedPlanShapeFails()
    {
        assertUpdate("CREATE MATERIALIZED VIEW mv_replace WITH (substitution_enabled = true) AS SELECT id FROM base_table");
        try {
            assertQueryFails(
                    "CREATE OR REPLACE MATERIALIZED VIEW mv_replace WITH (substitution_enabled = true) AS SELECT id, random() AS r FROM base_table",
                    NOT_A_CANDIDATE);
            assertThat((String) computeScalar("SHOW CREATE MATERIALIZED VIEW mv_replace"))
                    .as("The rejected replacement must not have been applied")
                    .contains("SELECT id")
                    .doesNotContain("random");
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW mv_replace");
        }
    }

    @Test
    public void testRefreshFailsWhenViewCanNoLongerBeIndexed()
    {
        // Substitution is validated when the materialized view is created, but the plan shape of its definition
        // query can change afterwards - here a view it reads is replaced by an unsupported one. The refresh can
        // no longer index the materialized view and must fail rather than silently leave substitution unusable.
        assertUpdate("CREATE TABLE refresh_src AS SELECT BIGINT '1' AS id", 1);
        assertUpdate("CREATE VIEW refresh_view AS SELECT id FROM refresh_src");
        try {
            assertUpdate("CREATE MATERIALIZED VIEW mv_refresh WITH (substitution_enabled = true) AS SELECT id FROM refresh_view");
            try {
                assertUpdate("REFRESH MATERIALIZED VIEW mv_refresh", 1);

                assertUpdate("CREATE OR REPLACE VIEW refresh_view AS SELECT random(BIGINT '100') AS id FROM refresh_src");
                // The insert makes the materialized view stale, so the refresh actually re-runs its query.
                assertUpdate("INSERT INTO refresh_src VALUES BIGINT '2'", 1);
                assertQueryFails(
                        "REFRESH MATERIALIZED VIEW mv_refresh",
                        ".*has substitution_enabled = true but cannot be indexed for substitution");

                // The connector committed the refresh before the indexing check ran, so the new data is there
                // even though the statement failed: the view now yields a row per source row, and the insert
                // above added one. The values themselves are random, so only the count is assertable.
                assertThat(computeActual("SELECT count(*) FROM mv_refresh").getOnlyValue()).isEqualTo(2L);

                // The candidate indexed by the earlier successful refresh pins the storage snapshot it captured,
                // and the failed refresh advanced that snapshot, so the candidate can no longer substitute. Were
                // it still to fire, this query would return the storage table's random values instead of the
                // source rows.
                MaterializedResultWithPlan sourceQuery = getDistributedQueryRunner().executeWithPlan(
                        sessionWithSubstitution(),
                        "SELECT id FROM refresh_src");
                assertThat(scannedTableNames(sourceQuery))
                        .as("Expected a scan on the source table, not on the materialized view storage")
                        .contains("refresh_src");
                assertThat(sourceQuery.result().getOnlyColumnAsSet()).containsExactlyInAnyOrder(1L, 2L);
            }
            finally {
                assertUpdate("DROP MATERIALIZED VIEW mv_refresh");
            }
        }
        finally {
            assertUpdate("DROP VIEW refresh_view");
            assertUpdate("DROP TABLE refresh_src");
        }
    }

    private Session sessionWithoutCandidateValidation()
    {
        return Session.builder(getSession())
                .setSystemProperty(MATERIALIZED_VIEW_SUBSTITUTION_CANDIDATE_VALIDATION_ENABLED, "false")
                .build();
    }

    private Session sessionWithSubstitution()
    {
        return Session.builder(getSession())
                .setSystemProperty(MATERIALIZED_VIEW_SUBSTITUTION_ENABLED, "true")
                .build();
    }

    private List<String> scannedTableNames(MaterializedResultWithPlan result)
    {
        return getDistributedQueryRunner().getCoordinator()
                .getQueryManager()
                .getFullQueryInfo(result.queryId())
                .getInputs().stream()
                .map(Input::table)
                .collect(toImmutableList());
    }

    @Test
    public void testSetPropertiesEnablingSubstitutionOnUnsupportedDefinitionFails()
    {
        assertUpdate("CREATE MATERIALIZED VIEW mv_alter WITH (substitution_enabled = false) AS SELECT id, random() AS r FROM base_table");
        try {
            assertQueryFails(
                    "ALTER MATERIALIZED VIEW mv_alter SET PROPERTIES substitution_enabled = true",
                    NOT_A_CANDIDATE);
            assertThat((String) computeScalar("SHOW CREATE MATERIALIZED VIEW mv_alter"))
                    .as("The rejected property change must not have been applied")
                    .contains("substitution_enabled = false");
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW mv_alter");
        }
    }

    @Test
    public void testSetPropertiesDisablingSubstitutionIsNotValidated()
    {
        assertUpdate("CREATE MATERIALIZED VIEW mv_alter_disable WITH (substitution_enabled = false) AS SELECT id, random() AS r FROM base_table");
        try {
            assertUpdate("ALTER MATERIALIZED VIEW mv_alter_disable SET PROPERTIES substitution_enabled = false");
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW mv_alter_disable");
        }
    }

    @Test
    public void testSetPropertiesEnablingSubstitutionOnSupportedDefinitionSucceeds()
    {
        assertUpdate("CREATE MATERIALIZED VIEW mv_alter_enable WITH (substitution_enabled = false) AS SELECT id FROM base_table");
        try {
            assertUpdate("ALTER MATERIALIZED VIEW mv_alter_enable SET PROPERTIES substitution_enabled = true");
            assertThat((String) computeScalar("SHOW CREATE MATERIALIZED VIEW mv_alter_enable"))
                    .contains("substitution_enabled = true");
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW mv_alter_enable");
        }
    }
}
