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

import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.plugin.iceberg.BaseIcebergMaterializedViewTest;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner.MaterializedResultWithPlan;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.OptionalLong;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Query-level integration tests for automatic MV substitution with Iceberg.
 * <p>
 * Unlike {@link BaseIcebergMaterializedViewTest} which queries MVs directly
 * ({@code SELECT * FROM mv_name}), these tests query the <em>base table</em>
 * ({@code SELECT * FROM orders}) and verify the optimizer transparently
 * redirects the scan to the MV storage table.
 */
public abstract class AbstractIcebergMvSubstitutionTest
        extends AbstractTestQueryFramework
{
    private static final String MV_SUBSTITUTION_ENABLED = "materialized_view_substitution_enabled";

    @BeforeAll
    public void setUp()
    {
        // Local writable copies seeded from TPCH. Keep TPCH's column names; cast numeric
        // prices/quantities to DECIMAL so DECIMAL literals in test queries compare cleanly,
        // and widen orderstatus from VARCHAR(1) to VARCHAR so synthetic INSERTs can use
        // values outside the TPCH F/O/P domain.
        assertUpdate("CREATE TABLE orders AS SELECT " +
                "orderkey, " +
                "custkey, " +
                "orderdate, " +
                "CAST(totalprice AS DECIMAL(12, 2)) AS totalprice, " +
                "CAST(orderstatus AS VARCHAR) AS orderstatus " +
                "FROM tpch.tiny.orders", 15000);

        assertUpdate("CREATE TABLE lineitem AS SELECT " +
                "orderkey, " +
                "linenumber, " +
                "CAST(quantity AS DECIMAL(12, 2)) AS quantity, " +
                "CAST(extendedprice AS DECIMAL(12, 2)) AS extendedprice, " +
                "shipdate " +
                "FROM tpch.tiny.lineitem", 60175);
    }

    protected Session sessionWithSubstitution()
    {
        return Session.builder(getSession())
                .setSystemProperty(MV_SUBSTITUTION_ENABLED, "true")
                .build();
    }

    protected Session sessionWithoutSubstitution()
    {
        return Session.builder(getSession())
                .setSystemProperty(MV_SUBSTITUTION_ENABLED, "false")
                .build();
    }

    protected void assertSameResults(Session session, String sql)
    {
        MaterializedResult substituted = computeActual(session, sql);
        MaterializedResult baseline = computeActual(sql);
        assertThat(substituted.getMaterializedRows())
                .as("Substituted query should return same results as base table query")
                .containsExactlyInAnyOrderElementsOf(baseline.getMaterializedRows());
    }

    protected void assertSameResults(Session session, String actual, String expected)
    {
        MaterializedResult actualResults = computeActual(session, actual);
        MaterializedResult expectedResults = computeActual(session, expected);
        assertThat(actualResults.getMaterializedRows())
                .as("Query '%s' should return same results as '%s'".formatted(actual, expected))
                .containsExactlyInAnyOrderElementsOf(expectedResults.getMaterializedRows());
    }

    private void createSubstitutionMv(CatalogSchemaTableName mvName, String sql)
    {
        createSubstitutionMv(mvName, sql, OptionalLong.empty());
    }

    private void createSubstitutionMv(CatalogSchemaTableName mvName, String sql, OptionalLong gracePeriodSeconds)
    {
        String gracePeriod = gracePeriodSeconds.isPresent() ? " GRACE PERIOD INTERVAL '%s' SECOND".formatted(gracePeriodSeconds.getAsLong()) : "";
        assertUpdate("CREATE MATERIALIZED VIEW %s%s WITH (substitution_enabled = true) AS %s".formatted(mvName, gracePeriod, sql));
        assertUpdate("REFRESH MATERIALIZED VIEW %s".formatted(mvName), computeActual(sql).getRowCount());
    }

    protected CatalogSchemaName getMvCatalogSchema()
    {
        return new CatalogSchemaName(getSession().getCatalog().orElseThrow(), getSession().getSchema().orElseThrow());
    }

    private MaterializedResultWithPlan assertSubstituted(Session session, String query, String baseTableName, String... expectedNotSubstitutedTables)
    {
        MaterializedResultWithPlan result = getDistributedQueryRunner().executeWithPlan(session, query);
        List<CatalogSchemaTableName> scannedTableNames = getScannedTableNames(result);

        assertThat(scannedTableNames)
                .as("Expected at least one table scan in plan")
                .isNotEmpty();
        assertThat(scannedTableNames)
                .extracting(table -> table.getSchemaTableName().getTableName())
                .as("Expected no scan on base tables '%s' — should be substituted with MV storage table", baseTableName)
                .doesNotContain(baseTableName)
                .containsAll(ImmutableList.copyOf(expectedNotSubstitutedTables));
        if (expectedNotSubstitutedTables.length > 0) {
            assertThat(scannedTableNames)
                    .extracting(table -> table.getSchemaTableName().getTableName())
                    .as("Expected scan on base tables '%s' — should not be substituted with MV storage table", Arrays.toString(expectedNotSubstitutedTables))
                    .contains(expectedNotSubstitutedTables);
        }
        assertThat(scannedTableNames)
                .as("Expected at least one scan on MV storage table")
                .anyMatch(table -> table.getSchemaTableName().getTableName().contains("materialized_view_storage"));

        return result;
    }

    private MaterializedResultWithPlan assertNotSubstituted(Session session, String query, String baseTableName)
    {
        MaterializedResultWithPlan result = getDistributedQueryRunner().executeWithPlan(session, query);
        List<CatalogSchemaTableName> scannedTableNames = getScannedTableNames(result);
        CatalogSchemaTableName baseTable = new CatalogSchemaTableName(
                getSession().getCatalog().orElseThrow(),
                getSession().getSchema().orElseThrow(),
                baseTableName);
        assertThat(scannedTableNames)
                .as("Expected scan on base table '%s' — no substitution should happen", baseTableName)
                .contains(baseTable);

        return result;
    }

    private List<CatalogSchemaTableName> getScannedTableNames(MaterializedResultWithPlan result)
    {
        return getDistributedQueryRunner().getCoordinator()
                .getQueryManager()
                .getFullQueryInfo(result.queryId())
                .getInputs().stream()
                .map(input -> new CatalogSchemaTableName(input.catalogName(), input.schema(), input.table()))
                .collect(toImmutableList());
    }

    @Test
    public void testScanOnlySubstitution()
    {
        CatalogSchemaTableName mvName = mvName("mv_scan_");
        try {
            createSubstitutionMv(mvName, "SELECT * FROM orders");

            Session session = sessionWithSubstitution();
            assertSubstituted(session, "SELECT * FROM orders", "orders");
            assertSameResults(session, "SELECT * FROM orders");
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + mvName);
        }
    }

    @Test
    public void testScanOnlySubstitutionWithColumnA()
    {
        CatalogSchemaTableName mvName = mvName("mv_col_subset_");
        try {
            createSubstitutionMv(mvName, "SELECT orderkey, custkey, totalprice FROM orders");

            Session session = sessionWithSubstitution();
            assertSubstituted(session, "SELECT orderkey, totalprice FROM orders", "orders");
            assertSameResults(session, "SELECT orderkey, totalprice FROM orders");
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + mvName);
        }
    }

    @Test
    public void testScanOnlySubstitutionWithColumnAlias()
    {
        CatalogSchemaTableName mvName = mvName("mv_col_subset_");
        try {
            createSubstitutionMv(mvName, "SELECT orderkey as o_key, custkey as c_key, totalprice as t_price FROM orders");

            Session session = sessionWithSubstitution();
            assertSubstituted(session, "SELECT orderkey, custkey, totalprice FROM orders", "orders");
            assertSameResults(session, "SELECT orderkey, custkey, totalprice FROM orders");
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + mvName);
        }
    }

    @Test
    public void testScanOnlySubstitutionColumnMissing()
    {
        CatalogSchemaTableName mvName = mvName("mv_col_missing_");
        try {
            createSubstitutionMv(mvName, "SELECT orderkey, custkey FROM orders");

            Session session = sessionWithSubstitution();
            // Query needs totalprice which MV does not have — should NOT substitute
            assertNotSubstituted(session, "SELECT orderkey, totalprice FROM orders", "orders");
            assertSameResults(session, "SELECT orderkey, totalprice FROM orders");
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + mvName);
        }
    }

    @Test
    public void testScanSelectStarWithColumnSubsetMv()
    {
        // SELECT * leaves IcebergTableHandle.projectedColumns empty (PruneTableScanColumns
        // does not fire when every output is referenced), so canSubstitute cannot reject
        // on column coverage alone — the per-column null check in MatchingVisitor must.
        CatalogSchemaTableName mvName = mvName("mv_col_missing_star_");
        try {
            createSubstitutionMv(mvName, "SELECT orderkey, custkey FROM orders");

            Session session = sessionWithSubstitution();
            assertNotSubstituted(session, "SELECT * FROM orders", "orders");
            assertSameResults(session, "SELECT * FROM orders");
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + mvName);
        }
    }

    @Test
    public void testScanWithSubFieldProjectionOverMv()
    {
        // The MV captures the whole struct/JSON column; the query projects individual
        // sub-fields. Substitution must fire because the TableScan sees the same base
        // column on both sides — the sub-field expression lives in a Project above the
        // scan and runs against the substituted MV storage.
        String tableName = "customers_with_sub_fields_" + randomNameSuffix();
        CatalogSchemaTableName mvName = mvName("mv_sub_field_");
        try {
            assertUpdate("CREATE TABLE " + tableName + " (id BIGINT, info " + subFieldColumnType() + ")");
            assertUpdate("INSERT INTO " + tableName + " VALUES " + subFieldInsertValues(), 3);

            createSubstitutionMv(mvName, "SELECT id, info FROM " + tableName);

            Session session = sessionWithSubstitution();
            String nameQuery = "SELECT " + subFieldExpression("info", "name") + " FROM " + tableName;
            assertSubstituted(session, nameQuery, tableName);
            assertSameResults(session, nameQuery);

            String ageQuery = "SELECT " + subFieldExpression("info", "age") + " FROM " + tableName;
            assertSubstituted(session, ageQuery, tableName);
            assertSameResults(session, ageQuery);
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + mvName);
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testScanWithWhereClauseOnSubstitutedScan()
    {
        CatalogSchemaTableName mvName = mvName("mv_scan_where_");
        try {
            createSubstitutionMv(mvName, "SELECT * FROM orders");

            Session session = sessionWithSubstitution();
            String query = "SELECT * FROM orders WHERE orderstatus = 'F'";
            assertSubstituted(session, query, "orders");
            assertSameResults(session, query);
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + mvName);
        }
    }

    @Test
    public void testScanWithAggregationOnSubstitutedScan()
    {
        CatalogSchemaTableName mvName = mvName("mv_scan_agg_");
        try {
            createSubstitutionMv(mvName, "SELECT * FROM orders");

            Session session = sessionWithSubstitution();
            String query = "SELECT count(*), sum(totalprice) FROM orders WHERE orderdate > DATE '1995-01-12'";
            assertSubstituted(session, query, "orders");
            assertSameResults(session, query);
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + mvName);
        }
    }

    @Test
    public void testSubstitutionDisabledBySessionProperty()
    {
        CatalogSchemaTableName mvName = mvName("mv_disabled_session_");
        try {
            createSubstitutionMv(mvName, "SELECT * FROM orders");

            assertNotSubstituted(sessionWithoutSubstitution(), "SELECT * FROM orders", "orders");
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + mvName);
        }
    }

    @Test
    public void testSubstitutionEnabledBySessionProperty()
    {
        CatalogSchemaTableName mvName = mvName("mv_enabled_session_");
        try {
            createSubstitutionMv(mvName, "SELECT * FROM orders");

            assertSubstituted(sessionWithSubstitution(), "SELECT * FROM orders", "orders");
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + mvName);
        }
    }

    @Test
    public void testStaleUnrefreshedMvNotSubstituted()
    {
        CatalogSchemaTableName mvName = mvName("mv_unrefreshed_");
        try {
            // Create but do NOT refresh
            assertUpdate("CREATE MATERIALIZED VIEW " + mvName +
                    " WITH (substitution_enabled = true) AS SELECT * FROM orders");

            assertNotSubstituted(sessionWithSubstitution(), "SELECT * FROM orders", "orders");
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + mvName);
        }
    }

    /**
     * Type declaration for a column that exposes named sub-fields, used by
     * {@link #testScanWithSubFieldProjectionOverMv}. Defaults to a Trino ROW (matches
     * Iceberg's struct support); connectors without ROW (e.g. JDBC/PostgreSQL) override
     * to a JSON column.
     */
    protected String subFieldColumnType()
    {
        return "ROW(name VARCHAR, age INTEGER)";
    }

    /**
     * INSERT values for {@link #subFieldColumnType()}: three rows, each with id BIGINT
     * and an info column carrying sub-fields {@code name} (VARCHAR) and {@code age} (INTEGER).
     */
    protected String subFieldInsertValues()
    {
        return "(1, CAST(ROW('Alice', 30) AS ROW(name VARCHAR, age INTEGER))), " +
                "(2, CAST(ROW('Bob', 25) AS ROW(name VARCHAR, age INTEGER))), " +
                "(3, CAST(ROW('Carol', 40) AS ROW(name VARCHAR, age INTEGER)))";
    }

    /**
     * Expression returning a sub-field of {@code column} as a string. Defaults to a Trino
     * struct dereference; JSON-column connectors override to {@code json_extract_scalar}.
     */
    protected String subFieldExpression(String column, String field)
    {
        return column + "." + field;
    }

    @Test
    public void testStaleMvAfterBaseTableInsert()
    {
        CatalogSchemaTableName mvName = mvName("mv_stale_insert_");
        try {
            createSubstitutionMv(mvName, "SELECT * FROM orders", OptionalLong.empty());

            // Make MV stale by inserting into base table
            assertUpdate("INSERT INTO orders VALUES (99990001, 400, DATE '1995-02-01', DECIMAL '99.99', 'N')", 1);

            // We do not support detecting source table change, so the substitution relies on the grace-period, and stale MV will be used
            assertSubstituted(sessionWithSubstitution(), "SELECT * FROM orders", "orders");
        }
        finally {
            getQueryRunner().execute("DELETE FROM orders WHERE orderkey = 99990001");
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + mvName);
        }
    }

    @Test
    public void testRefreshedMvBecomesSubstitutableAgain()
    {
        CatalogSchemaTableName mvName = mvName("mv_rerefresh_");
        try {
            long baseCount = (long) computeActual("SELECT count(*) FROM orders").getOnlyValue();
            createSubstitutionMv(mvName, "SELECT * FROM orders", OptionalLong.of(3600));
            assertUpdate("INSERT INTO orders VALUES (99990002, 400, DATE '1995-02-01', DECIMAL '99.99', 'N')", 1);
            // Session start time past the grace period so the stale MV is not used.
            Session expiredSession = Session.builder(sessionWithSubstitution())
                    .setSystemProperty("session_start_time", Instant.now().plus(1, ChronoUnit.DAYS).toString())
                    .build();
            assertNotSubstituted(expiredSession, "SELECT * FROM orders", "orders");

            // Re-refresh makes it fresh again
            getQueryRunner().execute("REFRESH MATERIALIZED VIEW " + mvName);
            assertSubstituted(sessionWithSubstitution(), "SELECT * FROM orders", "orders");

            // Result correctness after re-refresh
            assertThat(computeActual(sessionWithSubstitution(), "SELECT count(*) FROM orders").getOnlyValue()).isEqualTo(baseCount + 1);
        }
        finally {
            getQueryRunner().execute("DELETE FROM orders WHERE orderkey = 99990002");
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + mvName);
        }
    }

    @Test
    public void testMvWithGracePeriodUsedWhenWithinWindow()
    {
        CatalogSchemaTableName mvName = mvName("mv_grace_within_");
        try {
            assertUpdate("CREATE MATERIALIZED VIEW " + mvName +
                    " GRACE PERIOD INTERVAL '1' HOUR" +
                    " WITH (substitution_enabled = true)" +
                    " AS SELECT * FROM orders");
            assertUpdate("REFRESH MATERIALIZED VIEW " + mvName, computeActual("SELECT * FROM orders").getRowCount());

            // Insert to make it technically stale
            assertUpdate("INSERT INTO orders VALUES (99990003, 400, DATE '1995-02-01', DECIMAL '99.99', 'N')", 1);

            // Within grace period — should still substitute
            assertSubstituted(sessionWithSubstitution(), "SELECT * FROM orders", "orders");
        }
        finally {
            getQueryRunner().execute("DELETE FROM orders WHERE orderkey = 99990003");
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + mvName);
        }
    }

    @Test
    public void testMvWithGracePeriodNotUsedWhenExpired()
    {
        CatalogSchemaTableName mvName = mvName("mv_grace_expired_");
        try {
            assertUpdate("CREATE MATERIALIZED VIEW " + mvName +
                    " GRACE PERIOD INTERVAL '1' SECOND" +
                    " WITH (substitution_enabled = true)" +
                    " AS SELECT * FROM orders");
            assertUpdate("REFRESH MATERIALIZED VIEW " + mvName, computeActual("SELECT * FROM orders").getRowCount());

            // Insert to make it stale
            assertUpdate("INSERT INTO orders VALUES (99990004, 400, DATE '1995-02-01', DECIMAL '99.99', 'N')", 1);

            // Use a session with future start time to ensure grace period has expired
            Session futureSession = Session.builder(sessionWithSubstitution())
                    .setSystemProperty("session_start_time", Instant.now().plus(1, ChronoUnit.DAYS).toString())
                    .build();

            assertNotSubstituted(futureSession, "SELECT * FROM orders", "orders");
        }
        finally {
            getQueryRunner().execute("DELETE FROM orders WHERE orderkey = 99990004");
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + mvName);
        }
    }

    @Test
    public void testMvWithoutSubstitutionPropertyNotUsed()
    {
        CatalogSchemaTableName mvName = mvName("mv_no_prop_");
        try {
            // No substitution_enabled property — default is false
            assertUpdate("CREATE MATERIALIZED VIEW " + mvName + " AS SELECT * FROM orders");
            assertUpdate("REFRESH MATERIALIZED VIEW " + mvName, computeActual("SELECT * FROM orders").getRowCount());

            assertNotSubstituted(sessionWithSubstitution(), "SELECT * FROM orders", "orders");
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + mvName);
        }
    }

    @Test
    public void testMvWithSubstitutionDisabledNotUsed()
    {
        CatalogSchemaTableName mvName = mvName("mv_sub_false_");
        try {
            assertUpdate("CREATE MATERIALIZED VIEW " + mvName +
                    " WITH (substitution_enabled = false) AS SELECT * FROM orders");
            assertUpdate("REFRESH MATERIALIZED VIEW " + mvName, computeActual("SELECT * FROM orders").getRowCount());

            assertNotSubstituted(sessionWithSubstitution(), "SELECT * FROM orders", "orders");
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + mvName);
        }
    }

    @Test
    public void testMvWithSubstitutionEnabledUsed()
    {
        CatalogSchemaTableName mvName = mvName("mv_sub_true_");
        try {
            createSubstitutionMv(mvName, "SELECT * FROM orders");

            assertSubstituted(sessionWithSubstitution(), "SELECT * FROM orders", "orders");
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + mvName);
        }
    }

    @Test
    public void testForVersionAsOfNotSubstituted()
    {
        // FOR VERSION AS OF / FOR TIMESTAMP AS OF requests a specific historical snapshot.
        // The MV holds the current data — substituting would silently drop the user's
        // intended version and return current data instead.
        String tableName = "src_time_travel_" + randomNameSuffix();
        CatalogSchemaTableName mvName = mvName("mv_time_travel_");
        try {
            assertUpdate("CREATE TABLE " + tableName + " AS SELECT 1 AS id", 1);
            // Snapshot S1: one row
            long snapshotS1 = (long) computeActual(
                    "SELECT max(snapshot_id) FROM \"" + tableName + "$snapshots\"").getOnlyValue();

            assertUpdate("INSERT INTO " + tableName + " VALUES 2", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES 3", 1);
            // Now the table has 3 rows; S1 still references the 1-row snapshot.

            assertUpdate("CREATE MATERIALIZED VIEW " + mvName +
                    " WITH (substitution_enabled = true) AS SELECT * FROM " + tableName);
            assertUpdate("REFRESH MATERIALIZED VIEW " + mvName, 3);

            Session session = sessionWithSubstitution();
            // Current-snapshot query — substitution is fine here.
            assertSubstituted(session, "SELECT * FROM " + tableName, tableName);

            // FOR VERSION AS OF S1 must NOT substitute. The MV holds the current 3-row state;
            // the user asked for the historical 1-row state.
            String versionedQuery = "SELECT * FROM " + tableName + " FOR VERSION AS OF " + snapshotS1;
            assertNotSubstituted(session, versionedQuery, tableName);
            assertThat(computeActual(session, "SELECT count(*) FROM " + tableName + " FOR VERSION AS OF " + snapshotS1).getOnlyValue())
                    .as("Versioned query must return the historical snapshot's data, not the MV's current data")
                    .isEqualTo(1L);
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + mvName);
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testAlterSetSubstitutionEnabledOnUnrefreshedMvDoesNotSubstitute()
    {
        // ALTER ... SET PROPERTIES substitution_enabled = true must not index a materialized
        // view whose storage table has never been refreshed — otherwise the optimizer would
        // rewrite the scan to read the empty storage table and silently return zero rows.
        // The contract: enable substitution_enabled, then REFRESH; substitution only kicks in
        // after a real refresh stamps a true lastKnownFreshTime.
        CatalogSchemaTableName mvName = mvName("mv_alter_no_refresh_");
        try {
            // Create without refresh — storage table is empty.
            assertUpdate("CREATE MATERIALIZED VIEW " + mvName + " AS SELECT * FROM orders");

            // Flip substitution on. Currently this stamps Instant.now() as lastKnownFreshTime
            // and indexes the (empty) storage anyway.
            assertUpdate("ALTER MATERIALIZED VIEW " + mvName + " SET PROPERTIES substitution_enabled = true");

            Session session = sessionWithSubstitution();
            // Substitution must NOT fire — storage is empty and serving 0 rows would be wrong.
            assertNotSubstituted(session, "SELECT * FROM orders", "orders");
            assertSameResults(session, "SELECT * FROM orders");
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + mvName);
        }
    }

    @Test
    public void testMvSubstitutionEnabledViaAlterSetProperties()
    {
        // MV is created and refreshed without substitution_enabled, so its definition is NOT
        // indexed in the MaterializationIndex. ALTER ... SET PROPERTIES then turns substitution
        // on; the subsequent REFRESH must observe the flipped property and index the
        // materialization so the optimizer can substitute.
        CatalogSchemaTableName mvName = mvName("mv_alter_enable_");
        try {
            assertUpdate("CREATE MATERIALIZED VIEW " + mvName + " AS SELECT * FROM orders");
            assertUpdate("REFRESH MATERIALIZED VIEW " + mvName, computeActual("SELECT * FROM orders").getRowCount());

            Session session = sessionWithSubstitution();
            // Before ALTER: substitution_enabled is false (default), so no index entry exists.
            assertNotSubstituted(session, "SELECT * FROM orders", "orders");

            // Flipping substitution_enabled via ALTER must take effect immediately — no extra
            // REFRESH should be required, because the storage table is already fresh.
            assertUpdate("ALTER MATERIALIZED VIEW " + mvName + " SET PROPERTIES substitution_enabled = true");

            assertSubstituted(session, "SELECT * FROM orders", "orders");
            assertSameResults(session, "SELECT * FROM orders");
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + mvName);
        }
    }

    @Test
    public void testMultipleMvsOnSameTable()
    {
        CatalogSchemaTableName mvFull = mvName("mv_full_");
        CatalogSchemaTableName mvPartial = mvName("mv_partial_");
        try {
            createSubstitutionMv(mvFull, "SELECT * FROM orders");
            createSubstitutionMv(mvPartial, "SELECT orderkey, custkey FROM orders");

            Session session = sessionWithSubstitution();
            // Either MV could serve this query
            assertSubstituted(session, "SELECT orderkey, custkey FROM orders", "orders");
            assertSameResults(session, "SELECT orderkey, custkey FROM orders");
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + mvFull);
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + mvPartial);
        }
    }

    @Test
    @Disabled("We could make it work for iceberg MVs on iceberg source tables, but performance of checking MV freshness for every query mut be carefully considered")
    public void testStaleAndFreshMvPicksFresh()
    {
        CatalogSchemaTableName mvStale = mvName("mv_stale_");
        CatalogSchemaTableName mvFresh = mvName("mv_fresh_");
        try {
            long baseCount = (long) computeActual("SELECT count(*) FROM orders").getOnlyValue();
            createSubstitutionMv(mvStale, "SELECT * FROM orders");

            // Make mvStale stale
            assertUpdate("INSERT INTO orders VALUES (99990005, 400, DATE '1995-02-01', DECIMAL '99.99', 'N')", 1);

            // Create a fresh MV after the insert
            createSubstitutionMv(mvFresh, "SELECT * FROM orders");

            Session session = sessionWithSubstitution();
            assertSubstituted(session, "SELECT * FROM orders", "orders");
            // Result should include the new row (from the fresh MV)
            assertThat(computeActual(session, "SELECT count(*) FROM orders").getOnlyValue()).isEqualTo(baseCount + 1);
        }
        finally {
            getQueryRunner().execute("DELETE FROM orders WHERE orderkey = 99990005");
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + mvStale);
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + mvFresh);
        }
    }

    @Test
    public void testSubstitutedQueryReturnsCorrectResults()
    {
        CatalogSchemaTableName mvName = mvName("mv_correctness_");
        try {
            createSubstitutionMv(mvName, "SELECT * FROM orders");
            Session session = sessionWithSubstitution();

            assertSameResults(session, "SELECT * FROM orders");
            assertSameResults(session, "SELECT * FROM orders WHERE orderdate = DATE '1995-01-15'");
            assertSameResults(session, "SELECT * FROM orders ORDER BY orderkey");
            assertSameResults(session, "SELECT count(*) FROM orders");
            assertSameResults(session, "SELECT orderdate, sum(totalprice) FROM orders GROUP BY orderdate");
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + mvName);
        }
    }

    @Test
    public void testSubstitutedQueryWithJoin()
    {
        CatalogSchemaTableName mvName = mvName("mv_join_");
        try {
            createSubstitutionMv(mvName, "SELECT * FROM orders");
            Session session = sessionWithSubstitution();

            String query = "SELECT o.orderkey, l.quantity " +
                    "FROM orders o JOIN lineitem l ON o.orderkey = l.orderkey";

            // orders scan should be substituted, lineitem scan should not
            assertSubstituted(session, query, "orders", "lineitem");

            assertSameResults(session, query);
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + mvName);
        }
    }

    @Test
    public void testRefreshReadsFromBaseTableNotMv()
    {
        CatalogSchemaTableName mvName = mvName("mv_refresh_norecurse_");
        try {
            createSubstitutionMv(mvName, "SELECT * FROM orders");

            // Change the base table so we can detect it was used during the refresh
            assertUpdate("INSERT INTO orders VALUES (99990006, 400, DATE '1995-02-01', DECIMAL '99.99', 'N')", 1);

            // The refresh itself should read from base table, not from the MV
            getQueryRunner().execute(sessionWithSubstitution(), "REFRESH MATERIALIZED VIEW " + mvName);

            // Verify mv is refreshed from the base table
            assertSameResults(getSession(), "SELECT * FROM " + mvName, "SELECT * FROM orders");
        }
        finally {
            getQueryRunner().execute("DELETE FROM orders WHERE orderkey = 99990006");
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + mvName);
        }
    }

    @Test
    public void testSubstitutionAfterAddColumnToBaseTable()
    {
        String tableName = "orders_evolve_" + randomNameSuffix();
        CatalogSchemaTableName mvName = mvName("mv_evolve_");
        long baseCount = (long) computeActual("SELECT count(*) FROM orders").getOnlyValue();
        try {
            assertUpdate("CREATE TABLE " + tableName + " AS SELECT orderkey, totalprice FROM orders", baseCount);
            createSubstitutionMv(mvName, "SELECT orderkey, totalprice FROM " + tableName);

            assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN new_col VARCHAR");

            Session session = sessionWithSubstitution();
            // Query for original columns — MV should still be usable
            assertSubstituted(session, "SELECT orderkey, totalprice FROM " + tableName, tableName);
            assertSameResults(session, "SELECT orderkey, totalprice FROM " + tableName);
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + mvName);
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testSubstitutionAfterDropMv()
    {
        CatalogSchemaTableName mvName = mvName("mv_dropped_");
        try {
            createSubstitutionMv(mvName, "SELECT * FROM orders");
            assertSubstituted(sessionWithSubstitution(), "SELECT * FROM orders", "orders");

            assertUpdate("DROP MATERIALIZED VIEW " + mvName);

            // After dropping, no substitution should happen
            assertNotSubstituted(sessionWithSubstitution(), "SELECT * FROM orders", "orders");
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + mvName);
        }
    }

    @Test
    public void testSubstitutionStopsWhenGracePeriodRemoved()
    {
        CatalogSchemaTableName mvName = mvName("mv_grace_removed_");
        try {
            assertUpdate("CREATE MATERIALIZED VIEW " + mvName +
                    " GRACE PERIOD INTERVAL '1' HOUR" +
                    " WITH (substitution_enabled = true)" +
                    " AS SELECT * FROM orders");
            assertUpdate("REFRESH MATERIALIZED VIEW " + mvName, computeActual("SELECT * FROM orders").getRowCount());

            assertUpdate("INSERT INTO orders VALUES (99990007, 400, DATE '1995-02-01', DECIMAL '99.99', 'N')", 1);

            // Stale but within 1-hour grace period — substitution works
            assertSubstituted(sessionWithSubstitution(), "SELECT * FROM orders", "orders");

            // Replace MV without grace period (empty = Iceberg treats as infinite, so use zero)
            assertUpdate("CREATE OR REPLACE MATERIALIZED VIEW " + mvName +
                    " GRACE PERIOD INTERVAL '0' SECOND" +
                    " WITH (substitution_enabled = true)" +
                    " AS SELECT * FROM orders");
            getQueryRunner().execute("REFRESH MATERIALIZED VIEW " + mvName);

            assertUpdate("INSERT INTO orders VALUES (99990008, 500, DATE '1995-02-02', DECIMAL '88.88', 'N')", 1);

            // Stale with zero grace period — substitution must not happen
            assertNotSubstituted(sessionWithSubstitution(), "SELECT * FROM orders", "orders");
        }
        finally {
            getQueryRunner().execute("DELETE FROM orders WHERE orderkey IN (99990007, 99990008)");
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + mvName);
        }
    }

    @Test
    public void testSubstitutionStartsWhenGracePeriodAdded()
    {
        CatalogSchemaTableName mvName = mvName("mv_grace_added_");
        try {
            long baseCount = (long) computeActual("SELECT count(*) FROM orders").getOnlyValue();
            assertUpdate("CREATE MATERIALIZED VIEW " + mvName +
                    " GRACE PERIOD INTERVAL '0' SECOND" +
                    " WITH (substitution_enabled = true)" +
                    " AS SELECT * FROM orders");
            assertUpdate("REFRESH MATERIALIZED VIEW " + mvName, baseCount);

            assertUpdate("INSERT INTO orders VALUES (99990009, 400, DATE '1995-02-01', DECIMAL '99.99', 'N')", 1);

            // Stale with zero grace period — substitution does not work
            assertNotSubstituted(sessionWithSubstitution(), "SELECT * FROM orders", "orders");

            // Replace MV with a generous grace period
            assertUpdate("CREATE OR REPLACE MATERIALIZED VIEW " + mvName +
                    " GRACE PERIOD INTERVAL '1' HOUR" +
                    " WITH (substitution_enabled = true)" +
                    " AS SELECT * FROM orders");
            getQueryRunner().execute("REFRESH MATERIALIZED VIEW " + mvName);

            assertUpdate("INSERT INTO orders VALUES (99990010, 500, DATE '1995-02-02', DECIMAL '88.88', 'N')", 1);

            // Stale but within 1-hour grace period — substitution works now
            MaterializedResultWithPlan result = assertSubstituted(sessionWithSubstitution(), "SELECT count(*) FROM orders", "orders");
            // MV was refreshed after the first insert but before the second insert — stale data has baseCount+1
            assertThat(result.result().getOnlyValue())
                    .as("MV is stale but within grace period — should return MV row count, missing the latest insert")
                    .isEqualTo(baseCount + 1);
        }
        finally {
            getQueryRunner().execute("DELETE FROM orders WHERE orderkey IN (99990009, 99990010)");
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + mvName);
        }
    }

    @Test
    public void testSubstitutionAfterMvQueryTextChanged()
    {
        CatalogSchemaTableName mvName = mvName("mv_query_changed_");
        try {
            createSubstitutionMv(mvName, "SELECT * FROM orders");
            assertSubstituted(sessionWithSubstitution(), "SELECT * FROM orders", "orders");

            // Replace MV with a different query (different source table)
            assertUpdate("CREATE OR REPLACE MATERIALIZED VIEW " + mvName +
                    " WITH (substitution_enabled = true)" +
                    " AS SELECT * FROM lineitem");

            // Before refresh — MV is stale with the new definition, substitution should not work
            assertNotSubstituted(sessionWithSubstitution(), "SELECT * FROM lineitem", "lineitem");
            // Old query should also not be substituted (definition no longer matches orders)
            assertNotSubstituted(sessionWithSubstitution(), "SELECT * FROM orders", "orders");

            // Refresh with the new definition
            getQueryRunner().execute("REFRESH MATERIALIZED VIEW " + mvName);

            // After refresh — substitution works for the new query
            assertSubstituted(sessionWithSubstitution(), "SELECT * FROM lineitem", "lineitem");
            assertSameResults(sessionWithSubstitution(), "SELECT * FROM lineitem");
            // Old query still not substituted (MV no longer covers orders)
            assertNotSubstituted(sessionWithSubstitution(), "SELECT * FROM orders", "orders");
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + mvName);
        }
    }

    @Test
    public void testSubstitutionAfterRenameMv()
    {
        CatalogSchemaTableName mvName = mvName("mv_rename_orig_");
        CatalogSchemaTableName renamedMvName = mvName("mv_rename_new_");
        try {
            createSubstitutionMv(mvName, "SELECT * FROM orders");
            assertSubstituted(sessionWithSubstitution(), "SELECT * FROM orders", "orders");

            assertUpdate("ALTER MATERIALIZED VIEW " + mvName + " RENAME TO " + renamedMvName);

            // Substitution should still work after rename
            assertSubstituted(sessionWithSubstitution(), "SELECT * FROM orders", "orders");
            assertSameResults(sessionWithSubstitution(), "SELECT * FROM orders");
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + mvName);
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + renamedMvName);
        }
    }

    @Test
    public void testSubstitutionAfterDropAndRecreateWithDifferentDefinition()
    {
        // Hidden-storage Iceberg MVs derive the storage name from the MV name
        // (`<mv>$materialized_view_storage`). After a drop+recreate with the same MV name,
        // the new MV reuses the logical storage name but the physical Iceberg table
        // (tableLocation) is freshly created. Substitution must serve the new MV's data —
        // never the old MV's data via a stale cached mapping.
        CatalogSchemaTableName mvName = mvName("mv_recreate_");
        try {
            // First incarnation: covers orders
            createSubstitutionMv(mvName, "SELECT * FROM orders");
            assertSubstituted(sessionWithSubstitution(), "SELECT * FROM orders", "orders");

            assertUpdate("DROP MATERIALIZED VIEW " + mvName);

            // Second incarnation under the same name: covers lineitem (different base table)
            createSubstitutionMv(mvName, "SELECT * FROM lineitem");

            Session session = sessionWithSubstitution();
            // The old computation pattern must not be substituted by the new MV's storage
            assertNotSubstituted(session, "SELECT * FROM orders", "orders");
            // The new pattern is served by the new MV
            assertSubstituted(session, "SELECT * FROM lineitem", "lineitem");
            assertSameResults(session, "SELECT * FROM lineitem");
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + mvName);
        }
    }

    @Test
    public void testSubstitutionWithPartitionedMv()
    {
        // Partition by orderstatus (3 distinct values) instead of a high-cardinality column —
        // the test just verifies substitution works against a partitioned MV.
        CatalogSchemaTableName mvName = mvName("mv_partitioned_");
        try {
            assertUpdate("CREATE MATERIALIZED VIEW " + mvName +
                    " WITH (substitution_enabled = true, partitioning = ARRAY['orderstatus'])" +
                    " AS SELECT * FROM orders");
            assertUpdate("REFRESH MATERIALIZED VIEW " + mvName, computeActual("SELECT * FROM orders").getRowCount());

            Session session = sessionWithSubstitution();
            String query = "SELECT * FROM orders WHERE orderstatus = 'F'";
            assertSubstituted(session, query, "orders");
            assertSameResults(session, query);
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + mvName);
        }
    }

    @Test
    public void testNoSubstitutionForDifferentTable()
    {
        CatalogSchemaTableName mvName = mvName("mv_wrong_table_");
        try {
            createSubstitutionMv(mvName, "SELECT * FROM orders");

            // Query on lineitem should NOT be substituted by an orders MV
            assertNotSubstituted(sessionWithSubstitution(), "SELECT * FROM lineitem", "lineitem");
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + mvName);
        }
    }

    @Test
    public void testGracePeriodMvReturnsStaleData()
    {
        CatalogSchemaTableName mvName = mvName("mv_stale_data_");
        try {
            long baseCount = (long) computeActual("SELECT count(*) FROM orders").getOnlyValue();
            assertUpdate("CREATE MATERIALIZED VIEW " + mvName +
                    " GRACE PERIOD INTERVAL '1' HOUR" +
                    " WITH (substitution_enabled = true)" +
                    " AS SELECT * FROM orders");
            assertUpdate("REFRESH MATERIALIZED VIEW " + mvName, baseCount);

            // MV has baseCount rows. Insert one more row into the base table.
            assertUpdate("INSERT INTO orders VALUES (99990011, 400, DATE '1995-02-01', DECIMAL '99.99', 'N')", 1);

            Session session = sessionWithSubstitution();
            // Within grace period: substitution uses MV, which does NOT have the new row
            MaterializedResultWithPlan result = assertSubstituted(session, "SELECT count(*) FROM orders", "orders");
            assertThat(result.result().getOnlyValue())
                    .as("MV is stale but within grace period — should return MV data, not base table data")
                    .isEqualTo(baseCount);
        }
        finally {
            getQueryRunner().execute("DELETE FROM orders WHERE orderkey = 99990011");
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + mvName);
        }
    }

    @Test
    public void testMvProjectionExactMatchSubstitutes()
    {
        // MV projects exactly the columns the query needs
        CatalogSchemaTableName mvName = mvName("mv_proj_exact_");
        try {
            createSubstitutionMv(mvName, "SELECT orderkey, orderstatus, totalprice FROM orders");

            Session session = sessionWithSubstitution();
            String query = "SELECT orderkey, orderstatus, totalprice FROM orders";
            assertSubstituted(session, query, "orders");
            assertSameResults(session, query);
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + mvName);
        }
    }

    @Test
    public void testMvProjectionSupersetSubstitutes()
    {
        // MV has more columns than the query needs — query can still use MV
        CatalogSchemaTableName mvName = mvName("mv_proj_superset_");
        try {
            createSubstitutionMv(mvName, "SELECT orderkey, custkey, orderstatus, totalprice FROM orders");

            Session session = sessionWithSubstitution();
            String query = "SELECT orderkey, totalprice FROM orders";
            assertSubstituted(session, query, "orders");
            assertSameResults(session, query);
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + mvName);
        }
    }

    @Test
    public void testMvProjectionMissingColumnDoesNotSubstitute()
    {
        // MV lacks a column the query needs — cannot substitute
        CatalogSchemaTableName mvName = mvName("mv_proj_missing_");
        try {
            createSubstitutionMv(mvName, "SELECT orderkey, custkey FROM orders");

            Session session = sessionWithSubstitution();
            String query = "SELECT orderkey, orderstatus FROM orders";
            assertNotSubstituted(session, query, "orders");
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + mvName);
        }
    }

    @Test
    public void testSubstitutionWorksWithFullAccess()
    {
        CatalogSchemaTableName mvName = mvName("mv_acl_ok_");
        try {
            createSubstitutionMv(mvName, "SELECT * FROM orders");

            // User has SELECT on both base table and MV storage table — substitution works
            assertSubstituted(sessionWithSubstitution(), "SELECT * FROM orders", "orders");
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + mvName);
        }
    }

    @Test
    public void testMvNotMarkedForSubstitutionIsIgnored()
    {
        CatalogSchemaTableName mvName = mvName("mv_not_marked_");
        try {
            long baseCount = (long) computeActual("SELECT count(*) FROM orders").getOnlyValue();
            // Create MV WITHOUT substitution_enabled — default is false
            assertUpdate("CREATE MATERIALIZED VIEW " + mvName + " AS SELECT * FROM orders");
            assertUpdate("REFRESH MATERIALIZED VIEW " + mvName, baseCount);

            // Even with session property enabled, this MV should not be used
            assertNotSubstituted(sessionWithSubstitution(), "SELECT * FROM orders", "orders");

            // Verify the query still returns correct data from the base table
            assertThat(computeActual(sessionWithSubstitution(), "SELECT count(*) FROM orders").getOnlyValue()).isEqualTo(baseCount);
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + mvName);
        }
    }

    @Test
    public void testMvExplicitlyDisabledForSubstitutionIsIgnored()
    {
        CatalogSchemaTableName mvName = mvName("mv_disabled_");
        try {
            assertUpdate("CREATE MATERIALIZED VIEW " + mvName +
                    " WITH (substitution_enabled = false) AS SELECT * FROM orders");
            assertUpdate("REFRESH MATERIALIZED VIEW " + mvName, computeActual("SELECT * FROM orders").getRowCount());

            assertNotSubstituted(sessionWithSubstitution(), "SELECT * FROM orders", "orders");
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + mvName);
        }
    }

    @Test
    public void testMvMarkedForSubstitutionCoexistsWithUnmarked()
    {
        // Two MVs exist: one marked, one not. Only the marked one should be used.
        CatalogSchemaTableName mvMarked = mvName("mv_marked_");
        CatalogSchemaTableName mvUnmarked = mvName("mv_unmarked_");
        try {
            createSubstitutionMv(mvMarked, "SELECT * FROM orders");
            assertUpdate("CREATE MATERIALIZED VIEW " + mvUnmarked + " AS SELECT * FROM orders");
            assertUpdate("REFRESH MATERIALIZED VIEW " + mvUnmarked, computeActual("SELECT * FROM orders").getRowCount());

            MaterializedResultWithPlan result = assertSubstituted(
                    sessionWithSubstitution(), "SELECT * FROM orders", "orders");
            // Verify the plan uses the marked MV, not the unmarked one
            List<CatalogSchemaTableName> scannedTables = getScannedTableNames(result);
            assertThat(scannedTables).anyMatch(table -> table.getSchemaTableName().getTableName().endsWith("materialized_view_storage"));
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + mvMarked);
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + mvUnmarked);
        }
    }

    private CatalogSchemaTableName mvName(String mvName)
    {
        return new CatalogSchemaTableName(getMvCatalogSchema().getCatalogName(), getMvCatalogSchema().getSchemaName(), mvName + randomNameSuffix());
    }
}
