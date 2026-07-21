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
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.metadata.MaterializedViewDefinition;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.plugin.iceberg.BaseIcebergMaterializedViewTest;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner.MaterializedResultWithPlan;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.OptionalLong;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.SELECT_COLUMN;
import static io.trino.testing.TestingAccessControlManager.privilege;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TransactionBuilder.transaction;
import static java.util.stream.Collectors.joining;
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

    // Random suffixes so concurrent runs against a shared catalog (e.g. a shared BigQuery dataset)
    // do not collide on the base table names.
    private final String ordersTable = "orders_" + randomNameSuffix();
    private final String lineitemTable = "lineitem_" + randomNameSuffix();

    @BeforeAll
    public void setUp()
    {
        // Base tables seeded from TPCH. Use only DOUBLE and VARCHAR columns (no DECIMAL/DATE) so
        // every connector under test — including ones with limited write type support like
        // Cassandra — can host them. orderdate/shipdate are stored as VARCHAR in ISO form, which
        // still compares lexically like a date. orderstatus is widened from VARCHAR(1) to VARCHAR
        // so synthetic INSERTs can use values outside the TPCH F/O/P domain.
        //
        // Tests that INSERT into a base table do not delete the added rows afterwards (some
        // connectors, e.g. Cassandra, cannot DELETE by an arbitrary predicate). Each test stays
        // independent by capturing its own baseline count instead of relying on a global row
        // count; concrete tests run single-threaded (@Execution(SAME_THREAD)).
        assertUpdate("CREATE TABLE " + ordersTable + " AS SELECT " +
                "orderkey, " +
                "custkey, " +
                "CAST(orderdate AS VARCHAR) AS orderdate, " +
                "totalprice, " +
                "CAST(orderstatus AS VARCHAR) AS orderstatus " +
                "FROM tpch.tiny.orders  where orderkey between 20000 and 20010", 8);

        assertUpdate("CREATE TABLE " + lineitemTable + " AS SELECT " +
                "orderkey, " +
                "linenumber, " +
                "quantity, " +
                "extendedprice, " +
                "CAST(shipdate AS VARCHAR) AS shipdate " +
                "FROM tpch.tiny.lineitem where orderkey between 20000 and 20010", 32);
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
        getQueryRunner().execute("REFRESH MATERIALIZED VIEW %s".formatted(mvName));
    }

    protected CatalogSchemaName getMvCatalogSchema()
    {
        return new CatalogSchemaName(getSession().getCatalog().orElseThrow(), getSession().getSchema().orElseThrow());
    }

    protected MaterializedResultWithPlan assertSubstituted(Session session, String query, String baseTableName, CatalogSchemaTableName mvName, String... expectedNotSubstitutedTables)
    {
        return assertSubstituted(session, query, baseTableName, Set.of(mvName), expectedNotSubstitutedTables);
    }

    protected MaterializedResultWithPlan assertSubstituted(
            Session session,
            String query,
            String baseTableName,
            Set<CatalogSchemaTableName> materializedViews,
            String... expectedNotSubstitutedTables)
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

        List<String> storageTableNames = materializedViews.stream()
                .map(mvName -> getMaterializedViewDefinition(session, mvName)
                        .getStorageTable()
                        .orElseThrow()
                        .getSchemaTableName()
                        .getTableName())
                .collect(toImmutableList());
        assertThat(scannedTableNames)
                .extracting(table -> table.getSchemaTableName().getTableName())
                .as("Expected at least one scan on MV storage table")
                .containsAnyElementsOf(storageTableNames);

        return result;
    }

    private MaterializedViewDefinition getMaterializedViewDefinition(Session session, CatalogSchemaTableName mvName)
    {
        Metadata metadata = getDistributedQueryRunner().getPlannerContext().getMetadata();

        return transaction(getQueryRunner().getTransactionManager(), metadata, getQueryRunner().getAccessControl())
                .readOnly()
                .execute(session, transactionSession -> {
                    QualifiedObjectName name = new QualifiedObjectName(
                            mvName.getCatalogName(),
                            mvName.getSchemaTableName().getSchemaName(),
                            mvName.getSchemaTableName().getTableName());
                    return metadata.getMaterializedView(transactionSession, name).orElseThrow();
                });
    }

    protected MaterializedResultWithPlan assertNotSubstituted(Session session, String query, String baseTableName)
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
    public void testFunctionProjectionCoercesStorageTypes()
    {
        // A source connector can report a column type that Iceberg normalizes when it stores the MV:
        // bounded varchar -> unbounded varchar, char -> varchar, smallint -> integer, time(p)/timestamp(p) -> microsecond precision,
        // and the same recursively inside array. A scalar function over the substituted column forces an
        // argument coercion whose resolved call records the exact source type, so substitution must scan
        // the storage column at its own type and cast it back up to the query type. Without the coercion
        // the substituted plan fails to type-check.
        List<CoercionColumn> columns = coercionColumns();
        String columnDefinitions = columns.stream()
                .map(column -> column.name() + " " + column.columnType())
                .collect(joining(", "));
        String values = columns.stream()
                .map(CoercionColumn::insertValue)
                .collect(joining(", "));
        String projectedColumns = columns.stream()
                .map(CoercionColumn::name)
                .collect(joining(", "));
        CatalogSchemaTableName mvName = mvName("mv_coercion_");
        try (TestTable testTable = newTrinoTable(
                "coercion_table_",
                "(id_col BIGINT, " + columnDefinitions + ")",
                List.of("1, " + values))) {
            String tableName = testTable.getName();
            createSubstitutionMv(mvName, "SELECT id_col, " + projectedColumns + " FROM " + tableName);

            Session session = sessionWithSubstitution();
            for (CoercionColumn column : columns) {
                String query = "SELECT " + column.projection().formatted(column.name()) + " FROM " + tableName;
                assertSubstituted(session, query, tableName, mvName);
                assertSameResults(session, query);
            }
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + mvName);
        }
    }

    /**
     * A base-table column whose source type Iceberg normalizes to a different storage type, together with
     * a projection template ({@code %s} is the column name) that applies a function forcing an argument
     * coercion to the exact source type - which reproduces the substitution type-check failure absent the
     * storage-type coercion.
     */
    public record CoercionColumn(String name, String columnType, String insertValue, String projection) {}

    protected List<CoercionColumn> coercionColumns()
    {
        return List.of(
                new CoercionColumn("c_varchar", "varchar(20)", "'Alice'", "upper(%s)"),
                new CoercionColumn("c_smallint", "smallint", "42", "abs(%s)"));
    }

    @Test
    public void testScanOnlySubstitution()
    {
        CatalogSchemaTableName mvName = mvName("mv_scan_");
        try {
            createSubstitutionMv(mvName, "SELECT * FROM " + ordersTable);

            Session session = sessionWithSubstitution();
            assertSubstituted(session, "SELECT * FROM " + ordersTable, ordersTable, mvName);
            assertSameResults(session, "SELECT * FROM " + ordersTable);
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
            createSubstitutionMv(mvName, "SELECT orderkey, custkey, totalprice FROM " + ordersTable);

            Session session = sessionWithSubstitution();
            assertSubstituted(session, "SELECT orderkey, totalprice FROM " + ordersTable, ordersTable, mvName);
            assertSameResults(session, "SELECT orderkey, totalprice FROM " + ordersTable);
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
            createSubstitutionMv(mvName, "SELECT orderkey as o_key, custkey as c_key, totalprice as t_price FROM " + ordersTable);

            Session session = sessionWithSubstitution();
            assertSubstituted(session, "SELECT orderkey, custkey, totalprice FROM " + ordersTable, ordersTable, mvName);
            assertSameResults(session, "SELECT orderkey, custkey, totalprice FROM " + ordersTable);
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
            createSubstitutionMv(mvName, "SELECT orderkey, custkey FROM " + ordersTable);

            Session session = sessionWithSubstitution();
            // Query needs totalprice which MV does not have — should NOT substitute
            assertNotSubstituted(session, "SELECT orderkey, totalprice FROM " + ordersTable, ordersTable);
            assertSameResults(session, "SELECT orderkey, totalprice FROM " + ordersTable);
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
            createSubstitutionMv(mvName, "SELECT orderkey, custkey FROM " + ordersTable);

            Session session = sessionWithSubstitution();
            assertNotSubstituted(session, "SELECT * FROM " + ordersTable, ordersTable);
            assertSameResults(session, "SELECT * FROM " + ordersTable);
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
            createNestedTypeTable(tableName);

            createSubstitutionMv(mvName, "SELECT id, info FROM " + tableName);

            Session session = sessionWithSubstitution();
            String nameQuery = "SELECT " + subFieldExpression("info", "name") + " FROM " + tableName;
            assertSubstituted(session, nameQuery, tableName, mvName);
            assertSameResults(session, nameQuery);

            String ageQuery = "SELECT " + subFieldExpression("info", "age") + " FROM " + tableName;
            assertSubstituted(session, ageQuery, tableName, mvName);
            assertSameResults(session, ageQuery);
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + mvName);
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    protected void createNestedTypeTable(String tableName)
    {
        assertUpdate("CREATE TABLE " + tableName + " (id BIGINT, info " + subFieldColumnType() + ")");
        assertUpdate("INSERT INTO " + tableName + " VALUES " + subFieldInsertValues(), 3);
    }

    @Test
    public void testScanWithWhereClauseOnSubstitutedScan()
    {
        CatalogSchemaTableName mvName = mvName("mv_scan_where_");
        try {
            createSubstitutionMv(mvName, "SELECT * FROM " + ordersTable);

            Session session = sessionWithSubstitution();
            String query = "SELECT * FROM " + ordersTable + " WHERE orderstatus = 'F'";
            assertSubstituted(session, query, ordersTable, mvName);
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
            createSubstitutionMv(mvName, "SELECT * FROM " + ordersTable);

            Session session = sessionWithSubstitution();
            String query = "SELECT count(*), sum(totalprice) FROM " + ordersTable + " WHERE orderdate > '1995-01-12'";
            assertSubstituted(session, query, ordersTable, mvName);
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
            createSubstitutionMv(mvName, "SELECT * FROM " + ordersTable);

            assertNotSubstituted(sessionWithoutSubstitution(), "SELECT * FROM " + ordersTable, ordersTable);
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
            createSubstitutionMv(mvName, "SELECT * FROM " + ordersTable);

            assertSubstituted(sessionWithSubstitution(), "SELECT * FROM " + ordersTable, ordersTable, mvName);
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
                    " WITH (substitution_enabled = true) AS SELECT * FROM " + ordersTable);

            assertNotSubstituted(sessionWithSubstitution(), "SELECT * FROM " + ordersTable, ordersTable);
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
            createSubstitutionMv(mvName, "SELECT * FROM " + ordersTable, OptionalLong.empty());

            // Make MV stale by inserting into base table
            assertUpdate("INSERT INTO " + ordersTable + " VALUES (99990001, 400, '1995-02-01', DOUBLE '99.99', 'N')", 1);

            // We do not support detecting source table change, so the substitution relies on the grace-period, and stale MV will be used
            assertSubstituted(sessionWithSubstitution(), "SELECT * FROM " + ordersTable, ordersTable, mvName);
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + mvName);
        }
    }

    @Test
    public void testRefreshedMvBecomesSubstitutableAgain()
    {
        CatalogSchemaTableName mvName = mvName("mv_rerefresh_");
        try {
            long baseCount = (long) computeActual("SELECT count(*) FROM " + ordersTable).getOnlyValue();
            createSubstitutionMv(mvName, "SELECT * FROM " + ordersTable, OptionalLong.of(3600));
            assertUpdate("INSERT INTO " + ordersTable + " VALUES (99990002, 400, '1995-02-01', DOUBLE '99.99', 'N')", 1);
            // Session start time past the grace period so the stale MV is not used.
            Session expiredSession = Session.builder(sessionWithSubstitution())
                    .setSystemProperty("session_start_time", Instant.now().plus(1, ChronoUnit.DAYS).toString())
                    .build();
            assertNotSubstituted(expiredSession, "SELECT * FROM " + ordersTable, ordersTable);

            // Re-refresh makes it fresh again
            getQueryRunner().execute("REFRESH MATERIALIZED VIEW " + mvName);
            assertSubstituted(sessionWithSubstitution(), "SELECT * FROM " + ordersTable, ordersTable, mvName);

            // Result correctness after re-refresh
            assertThat(computeActual(sessionWithSubstitution(), "SELECT count(*) FROM " + ordersTable).getOnlyValue()).isEqualTo(baseCount + 1);
        }
        finally {
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
                    " AS SELECT * FROM " + ordersTable);
            assertUpdate("REFRESH MATERIALIZED VIEW " + mvName, computeActual("SELECT * FROM " + ordersTable).getRowCount());

            // Insert to make it technically stale
            assertUpdate("INSERT INTO " + ordersTable + " VALUES (99990003, 400, '1995-02-01', DOUBLE '99.99', 'N')", 1);

            // Within grace period — should still substitute
            assertSubstituted(sessionWithSubstitution(), "SELECT * FROM " + ordersTable, ordersTable, mvName);
        }
        finally {
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
                    " AS SELECT * FROM " + ordersTable);
            assertUpdate("REFRESH MATERIALIZED VIEW " + mvName, computeActual("SELECT * FROM " + ordersTable).getRowCount());

            // Insert to make it stale
            assertUpdate("INSERT INTO " + ordersTable + " VALUES (99990004, 400, '1995-02-01', DOUBLE '99.99', 'N')", 1);

            // Use a session with future start time to ensure grace period has expired
            Session futureSession = Session.builder(sessionWithSubstitution())
                    .setSystemProperty("session_start_time", Instant.now().plus(1, ChronoUnit.DAYS).toString())
                    .build();

            assertNotSubstituted(futureSession, "SELECT * FROM " + ordersTable, ordersTable);
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + mvName);
        }
    }

    @Test
    public void testMvWithoutSubstitutionPropertyNotUsed()
    {
        CatalogSchemaTableName mvName = mvName("mv_no_prop_");
        try {
            // No substitution_enabled property — default is false
            assertUpdate("CREATE MATERIALIZED VIEW " + mvName + " AS SELECT * FROM " + ordersTable);
            assertUpdate("REFRESH MATERIALIZED VIEW " + mvName, computeActual("SELECT * FROM " + ordersTable).getRowCount());

            assertNotSubstituted(sessionWithSubstitution(), "SELECT * FROM " + ordersTable, ordersTable);
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
                    " WITH (substitution_enabled = false) AS SELECT * FROM " + ordersTable);
            assertUpdate("REFRESH MATERIALIZED VIEW " + mvName, computeActual("SELECT * FROM " + ordersTable).getRowCount());

            assertNotSubstituted(sessionWithSubstitution(), "SELECT * FROM " + ordersTable, ordersTable);
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
            createSubstitutionMv(mvName, "SELECT * FROM " + ordersTable);

            assertSubstituted(sessionWithSubstitution(), "SELECT * FROM " + ordersTable, ordersTable, mvName);
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
            assertSubstituted(session, "SELECT * FROM " + tableName, tableName, mvName);

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
            assertUpdate("CREATE MATERIALIZED VIEW " + mvName + " AS SELECT * FROM " + ordersTable);

            // Flip substitution on. Currently this stamps Instant.now() as lastKnownFreshTime
            // and indexes the (empty) storage anyway.
            assertUpdate("ALTER MATERIALIZED VIEW " + mvName + " SET PROPERTIES substitution_enabled = true");

            Session session = sessionWithSubstitution();
            // Substitution must NOT fire — storage is empty and serving 0 rows would be wrong.
            assertNotSubstituted(session, "SELECT * FROM " + ordersTable, ordersTable);
            assertSameResults(session, "SELECT * FROM " + ordersTable);
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
            assertUpdate("CREATE MATERIALIZED VIEW " + mvName + " AS SELECT * FROM " + ordersTable);
            assertUpdate("REFRESH MATERIALIZED VIEW " + mvName, computeActual("SELECT * FROM " + ordersTable).getRowCount());

            Session session = sessionWithSubstitution();
            // Before ALTER: substitution_enabled is false (default), so no index entry exists.
            assertNotSubstituted(session, "SELECT * FROM " + ordersTable, ordersTable);

            // Flipping substitution_enabled via ALTER must take effect immediately — no extra
            // REFRESH should be required, because the storage table is already fresh.
            assertUpdate("ALTER MATERIALIZED VIEW " + mvName + " SET PROPERTIES substitution_enabled = true");

            assertSubstituted(session, "SELECT * FROM " + ordersTable, ordersTable, mvName);
            assertSameResults(session, "SELECT * FROM " + ordersTable);
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
            createSubstitutionMv(mvFull, "SELECT * FROM " + ordersTable);
            createSubstitutionMv(mvPartial, "SELECT orderkey, custkey FROM " + ordersTable);

            Session session = sessionWithSubstitution();
            // Either MV could serve this query
            assertSubstituted(session, "SELECT orderkey, custkey FROM " + ordersTable, ordersTable, ImmutableSet.of(mvFull, mvPartial));
            assertSameResults(session, "SELECT orderkey, custkey FROM " + ordersTable);
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
            long baseCount = (long) computeActual("SELECT count(*) FROM " + ordersTable).getOnlyValue();
            createSubstitutionMv(mvStale, "SELECT * FROM " + ordersTable);

            // Make mvStale stale
            assertUpdate("INSERT INTO " + ordersTable + " VALUES (99990005, 400, '1995-02-01', DOUBLE '99.99', 'N')", 1);

            // Create a fresh MV after the insert
            createSubstitutionMv(mvFresh, "SELECT * FROM " + ordersTable);

            Session session = sessionWithSubstitution();
            assertSubstituted(session, "SELECT * FROM " + ordersTable, ordersTable, mvFresh);
            // Result should include the new row (from the fresh MV)
            assertThat(computeActual(session, "SELECT count(*) FROM " + ordersTable).getOnlyValue()).isEqualTo(baseCount + 1);
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + mvStale);
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + mvFresh);
        }
    }

    @Test
    public void testSubstitutedQueryReturnsCorrectResults()
    {
        CatalogSchemaTableName mvName = mvName("mv_correctness_");
        try {
            createSubstitutionMv(mvName, "SELECT * FROM " + ordersTable);
            Session session = sessionWithSubstitution();

            assertSameResults(session, "SELECT * FROM " + ordersTable);
            assertSameResults(session, "SELECT * FROM " + ordersTable + " WHERE orderdate = '1995-01-15'");
            assertSameResults(session, "SELECT * FROM " + ordersTable + " ORDER BY orderkey");
            assertSameResults(session, "SELECT count(*) FROM " + ordersTable);
            assertSameResults(session, "SELECT orderdate, sum(totalprice) FROM " + ordersTable + " GROUP BY orderdate");
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
            createSubstitutionMv(mvName, "SELECT * FROM " + ordersTable);
            Session session = sessionWithSubstitution();

            String query = "SELECT o.orderkey, l.quantity " +
                    "FROM " + ordersTable + " o JOIN " + lineitemTable + " l ON o.orderkey = l.orderkey";

            // orders scan should be substituted, lineitem scan should not
            assertSubstituted(session, query, ordersTable, mvName, lineitemTable);

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
            createSubstitutionMv(mvName, "SELECT * FROM " + ordersTable);

            // Change the base table so we can detect it was used during the refresh
            assertUpdate("INSERT INTO " + ordersTable + " VALUES (99990006, 400, '1995-02-01', DOUBLE '99.99', 'N')", 1);

            // The refresh itself should read from base table, not from the MV
            getQueryRunner().execute(sessionWithSubstitution(), "REFRESH MATERIALIZED VIEW " + mvName);

            // Verify mv is refreshed from the base table
            assertSameResults(getSession(), "SELECT * FROM " + mvName, "SELECT * FROM " + ordersTable);
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + mvName);
        }
    }

    @Test
    public void testSubstitutionAfterAddColumnToBaseTable()
    {
        String tableName = "orders_evolve_" + randomNameSuffix();
        CatalogSchemaTableName mvName = mvName("mv_evolve_");
        long baseCount = (long) computeActual("SELECT count(*) FROM " + ordersTable).getOnlyValue();
        try {
            assertUpdate("CREATE TABLE " + tableName + " AS SELECT orderkey, totalprice FROM " + ordersTable, baseCount);
            createSubstitutionMv(mvName, "SELECT orderkey, totalprice FROM " + tableName);

            assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN new_col VARCHAR");

            Session session = sessionWithSubstitution();
            // Query for original columns — MV should still be usable
            assertSubstituted(session, "SELECT orderkey, totalprice FROM " + tableName, tableName, mvName);
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
            createSubstitutionMv(mvName, "SELECT * FROM " + ordersTable);
            assertSubstituted(sessionWithSubstitution(), "SELECT * FROM " + ordersTable, ordersTable, mvName);

            assertUpdate("DROP MATERIALIZED VIEW " + mvName);

            // After dropping, no substitution should happen
            assertNotSubstituted(sessionWithSubstitution(), "SELECT * FROM " + ordersTable, ordersTable);
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
                    " AS SELECT * FROM " + ordersTable);
            assertUpdate("REFRESH MATERIALIZED VIEW " + mvName, computeActual("SELECT * FROM " + ordersTable).getRowCount());

            assertUpdate("INSERT INTO " + ordersTable + " VALUES (99990007, 400, '1995-02-01', DOUBLE '99.99', 'N')", 1);

            // Stale but within 1-hour grace period — substitution works
            assertSubstituted(sessionWithSubstitution(), "SELECT * FROM " + ordersTable, ordersTable, mvName);

            // Replace MV without grace period (empty = Iceberg treats as infinite, so use zero)
            assertUpdate("CREATE OR REPLACE MATERIALIZED VIEW " + mvName +
                    " GRACE PERIOD INTERVAL '0' SECOND" +
                    " WITH (substitution_enabled = true)" +
                    " AS SELECT * FROM " + ordersTable);
            getQueryRunner().execute("REFRESH MATERIALIZED VIEW " + mvName);

            assertUpdate("INSERT INTO " + ordersTable + " VALUES (99990008, 500, '1995-02-02', DOUBLE '88.88', 'N')", 1);

            // Stale with zero grace period — substitution must not happen
            assertNotSubstituted(sessionWithSubstitution(), "SELECT * FROM " + ordersTable, ordersTable);
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + mvName);
        }
    }

    @Test
    public void testSubstitutionStartsWhenGracePeriodAdded()
    {
        CatalogSchemaTableName mvName = mvName("mv_grace_added_");
        try {
            long baseCount = (long) computeActual("SELECT count(*) FROM " + ordersTable).getOnlyValue();
            assertUpdate("CREATE MATERIALIZED VIEW " + mvName +
                    " GRACE PERIOD INTERVAL '0' SECOND" +
                    " WITH (substitution_enabled = true)" +
                    " AS SELECT * FROM " + ordersTable);
            assertUpdate("REFRESH MATERIALIZED VIEW " + mvName, baseCount);

            assertUpdate("INSERT INTO " + ordersTable + " VALUES (99990009, 400, '1995-02-01', DOUBLE '99.99', 'N')", 1);

            // Stale with zero grace period — substitution does not work
            assertNotSubstituted(sessionWithSubstitution(), "SELECT * FROM " + ordersTable, ordersTable);

            // Replace MV with a generous grace period
            assertUpdate("CREATE OR REPLACE MATERIALIZED VIEW " + mvName +
                    " GRACE PERIOD INTERVAL '1' HOUR" +
                    " WITH (substitution_enabled = true)" +
                    " AS SELECT * FROM " + ordersTable);
            getQueryRunner().execute("REFRESH MATERIALIZED VIEW " + mvName);

            assertUpdate("INSERT INTO " + ordersTable + " VALUES (99990010, 500, '1995-02-02', DOUBLE '88.88', 'N')", 1);

            // Stale but within 1-hour grace period — substitution works now
            MaterializedResultWithPlan result = assertSubstituted(sessionWithSubstitution(), "SELECT count(*) FROM " + ordersTable, ordersTable, mvName);
            // MV was refreshed after the first insert but before the second insert — stale data has baseCount+1
            assertThat(result.result().getOnlyValue())
                    .as("MV is stale but within grace period — should return MV row count, missing the latest insert")
                    .isEqualTo(baseCount + 1);
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + mvName);
        }
    }

    @Test
    public void testSubstitutionAfterMvQueryTextChanged()
    {
        CatalogSchemaTableName mvName = mvName("mv_query_changed_");
        try {
            createSubstitutionMv(mvName, "SELECT * FROM " + ordersTable);
            assertSubstituted(sessionWithSubstitution(), "SELECT * FROM " + ordersTable, ordersTable, mvName);

            // Replace MV with a different query (different source table)
            assertUpdate("CREATE OR REPLACE MATERIALIZED VIEW " + mvName +
                    " WITH (substitution_enabled = true)" +
                    " AS SELECT * FROM " + lineitemTable);

            // Before refresh — MV is stale with the new definition, substitution should not work
            assertNotSubstituted(sessionWithSubstitution(), "SELECT * FROM " + lineitemTable, lineitemTable);
            // Old query should also not be substituted (definition no longer matches orders)
            assertNotSubstituted(sessionWithSubstitution(), "SELECT * FROM " + ordersTable, ordersTable);

            // Refresh with the new definition
            getQueryRunner().execute("REFRESH MATERIALIZED VIEW " + mvName);

            // After refresh — substitution works for the new query
            assertSubstituted(sessionWithSubstitution(), "SELECT * FROM " + lineitemTable, lineitemTable, mvName);
            assertSameResults(sessionWithSubstitution(), "SELECT * FROM " + lineitemTable);
            // Old query still not substituted (MV no longer covers orders)
            assertNotSubstituted(sessionWithSubstitution(), "SELECT * FROM " + ordersTable, ordersTable);
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
            createSubstitutionMv(mvName, "SELECT * FROM " + ordersTable);
            assertSubstituted(sessionWithSubstitution(), "SELECT * FROM " + ordersTable, ordersTable, mvName);

            assertUpdate("ALTER MATERIALIZED VIEW " + mvName + " RENAME TO " + renamedMvName);

            // Substitution should still work after rename
            assertSubstituted(sessionWithSubstitution(), "SELECT * FROM " + ordersTable, ordersTable, renamedMvName);
            assertSameResults(sessionWithSubstitution(), "SELECT * FROM " + ordersTable);
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
            createSubstitutionMv(mvName, "SELECT * FROM " + ordersTable);
            assertSubstituted(sessionWithSubstitution(), "SELECT * FROM " + ordersTable, ordersTable, mvName);

            assertUpdate("DROP MATERIALIZED VIEW " + mvName);

            // Second incarnation under the same name: covers lineitem (different base table)
            createSubstitutionMv(mvName, "SELECT * FROM " + lineitemTable);

            Session session = sessionWithSubstitution();
            // The old computation pattern must not be substituted by the new MV's storage
            assertNotSubstituted(session, "SELECT * FROM " + ordersTable, ordersTable);
            // The new pattern is served by the new MV
            assertSubstituted(session, "SELECT * FROM " + lineitemTable, lineitemTable, mvName);
            assertSameResults(session, "SELECT * FROM " + lineitemTable);
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
                    " AS SELECT * FROM " + ordersTable);
            assertUpdate("REFRESH MATERIALIZED VIEW " + mvName, computeActual("SELECT * FROM " + ordersTable).getRowCount());

            Session session = sessionWithSubstitution();
            String query = "SELECT * FROM " + ordersTable + " WHERE orderstatus = 'F'";
            assertSubstituted(session, query, ordersTable, mvName);
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
            createSubstitutionMv(mvName, "SELECT * FROM " + ordersTable);

            // Query on lineitem should NOT be substituted by an orders MV
            assertNotSubstituted(sessionWithSubstitution(), "SELECT * FROM " + lineitemTable, lineitemTable);
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
            long baseCount = (long) computeActual("SELECT count(*) FROM " + ordersTable).getOnlyValue();
            assertUpdate("CREATE MATERIALIZED VIEW " + mvName +
                    " GRACE PERIOD INTERVAL '1' HOUR" +
                    " WITH (substitution_enabled = true)" +
                    " AS SELECT * FROM " + ordersTable);
            assertUpdate("REFRESH MATERIALIZED VIEW " + mvName, baseCount);

            // MV has baseCount rows. Insert one more row into the base table.
            assertUpdate("INSERT INTO " + ordersTable + " VALUES (99990011, 400, '1995-02-01', DOUBLE '99.99', 'N')", 1);

            Session session = sessionWithSubstitution();
            // Within grace period: substitution uses MV, which does NOT have the new row
            MaterializedResultWithPlan result = assertSubstituted(session, "SELECT count(*) FROM " + ordersTable, ordersTable, mvName);
            assertThat(result.result().getOnlyValue())
                    .as("MV is stale but within grace period — should return MV data, not base table data")
                    .isEqualTo(baseCount);
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + mvName);
        }
    }

    @Test
    public void testMvProjectionExactMatchSubstitutes()
    {
        // MV projects exactly the columns the query needs
        CatalogSchemaTableName mvName = mvName("mv_proj_exact_");
        try {
            createSubstitutionMv(mvName, "SELECT orderkey, orderstatus, totalprice FROM " + ordersTable);

            Session session = sessionWithSubstitution();
            String query = "SELECT orderkey, orderstatus, totalprice FROM " + ordersTable;
            assertSubstituted(session, query, ordersTable, mvName);
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
            createSubstitutionMv(mvName, "SELECT orderkey, custkey, orderstatus, totalprice FROM " + ordersTable);

            Session session = sessionWithSubstitution();
            String query = "SELECT orderkey, totalprice FROM " + ordersTable;
            assertSubstituted(session, query, ordersTable, mvName);
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
            createSubstitutionMv(mvName, "SELECT orderkey, custkey FROM " + ordersTable);

            Session session = sessionWithSubstitution();
            String query = "SELECT orderkey, orderstatus FROM " + ordersTable;
            assertNotSubstituted(session, query, ordersTable);
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
            createSubstitutionMv(mvName, "SELECT * FROM " + ordersTable);

            // User has SELECT on both base table and MV storage table — substitution works
            assertSubstituted(sessionWithSubstitution(), "SELECT * FROM " + ordersTable, ordersTable, mvName);
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + mvName);
        }
    }

    @Test
    public void testSubstitutionFallsBackWhenMvAccessDenied()
    {
        // The user can read the base table but is denied access to the MV marked for substitution.
        // Substitution must not silently redirect the scan to the MV's storage table; it should fall
        // back to scanning the base table, which stays correct because base-table access is allowed.
        CatalogSchemaTableName mvName = mvName("mv_acl_mv_denied_");
        try {
            createSubstitutionMv(mvName, "SELECT * FROM " + ordersTable);

            getQueryRunner().getAccessControl().deny(privilege(mvName.getSchemaTableName().getTableName(), SELECT_COLUMN));
            try {
                assertNotSubstituted(sessionWithSubstitution(), "SELECT * FROM " + ordersTable, ordersTable);
            }
            finally {
                getQueryRunner().getAccessControl().reset();
            }
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + mvName);
        }
    }

    @Test
    public void testSubstitutionRespectsColumnLevelMvAccess()
    {
        // The user has SELECT on some MV columns but not others (here totalprice is denied).
        // Substitution applies only to queries that read the allowed columns; a query that reads
        // the denied column falls back to the base table (which the user can read in full).
        CatalogSchemaTableName mvName = mvName("mv_acl_col_");
        try {
            createSubstitutionMv(mvName, "SELECT * FROM " + ordersTable);

            getQueryRunner().getAccessControl().deny(privilege(mvName.getSchemaTableName().getTableName() + ".totalprice", SELECT_COLUMN));
            try {
                // Reads only allowed columns — substituted
                assertSubstituted(sessionWithSubstitution(), "SELECT orderkey, custkey FROM " + ordersTable, ordersTable, mvName);
                // Reads the denied column — falls back to the base table
                assertNotSubstituted(sessionWithSubstitution(), "SELECT totalprice FROM " + ordersTable, ordersTable);
            }
            finally {
                getQueryRunner().getAccessControl().reset();
            }
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
            long baseCount = (long) computeActual("SELECT count(*) FROM " + ordersTable).getOnlyValue();
            // Create MV WITHOUT substitution_enabled — default is false
            assertUpdate("CREATE MATERIALIZED VIEW " + mvName + " AS SELECT * FROM " + ordersTable);
            assertUpdate("REFRESH MATERIALIZED VIEW " + mvName, baseCount);

            // Even with session property enabled, this MV should not be used
            assertNotSubstituted(sessionWithSubstitution(), "SELECT * FROM " + ordersTable, ordersTable);

            // Verify the query still returns correct data from the base table
            assertThat(computeActual(sessionWithSubstitution(), "SELECT count(*) FROM " + ordersTable).getOnlyValue()).isEqualTo(baseCount);
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
                    " WITH (substitution_enabled = false) AS SELECT * FROM " + ordersTable);
            assertUpdate("REFRESH MATERIALIZED VIEW " + mvName, computeActual("SELECT * FROM " + ordersTable).getRowCount());

            assertNotSubstituted(sessionWithSubstitution(), "SELECT * FROM " + ordersTable, ordersTable);
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
            createSubstitutionMv(mvMarked, "SELECT * FROM " + ordersTable);
            assertUpdate("CREATE MATERIALIZED VIEW " + mvUnmarked + " AS SELECT * FROM " + ordersTable);
            assertUpdate("REFRESH MATERIALIZED VIEW " + mvUnmarked, computeActual("SELECT * FROM " + ordersTable).getRowCount());

            assertSubstituted(sessionWithSubstitution(), "SELECT * FROM " + ordersTable, ordersTable, mvMarked);
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
