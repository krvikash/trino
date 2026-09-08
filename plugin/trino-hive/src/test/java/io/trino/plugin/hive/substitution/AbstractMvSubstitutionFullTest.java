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
package io.trino.plugin.hive.substitution;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.testing.QueryRunner.MaterializedResultWithPlan;
import io.trino.testing.sql.TestMaterializedView;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;

import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.SELECT_COLUMN;
import static io.trino.testing.TestingAccessControlManager.privilege;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public abstract class AbstractMvSubstitutionFullTest
        extends AbstractMvSubstitutionTest
{
    // Random suffixes so concurrent runs against a shared catalog (e.g. a shared BigQuery dataset)
    // do not collide on the base table names.
    protected final CatalogSchemaTableName ordersTable = sourceTable("orders_" + randomNameSuffix());

    protected final CatalogSchemaTableName lineitemTable = sourceTable("lineitem_" + randomNameSuffix());

    // Table with a nested/sub-field column (see subFieldTestContext), used by the sub-field tests.
    protected final CatalogSchemaTableName nestedTypeTable = sourceTable("customers_with_sub_fields_" + randomNameSuffix());

    protected final CatalogSchemaTableName coercionTable = sourceTable("coercion_table_" + randomNameSuffix());

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
        // All source tables are created once here; individual tests only reference them, never
        // create their own. This lets read-only / static-schema connectors (e.g. Redis) provision
        // and seed every table up front and leave the create hooks as no-ops.
        createOrdersTable();

        createLineItemTable();

        createNestedTypeTable(nestedTypeTable);

        createCoercionTable();
    }

    protected void createOrdersTable()
    {
        assertUpdate("CREATE TABLE " + ordersTable + " " + sourceTablePropertiesClause() + " AS SELECT " +
                "orderkey, " +
                "custkey, " +
                "CAST(orderdate AS VARCHAR) AS orderdate, " +
                "totalprice, " +
                "CAST(orderstatus AS VARCHAR) AS orderstatus " +
                "FROM tpch.tiny.orders  where orderkey between 20000 and 20010", 8);
    }

    protected void createLineItemTable()
    {
        assertUpdate("CREATE TABLE " + lineitemTable + " " + sourceTablePropertiesClause() + " AS SELECT " +
                "orderkey, " +
                "linenumber, " +
                "quantity, " +
                "extendedprice, " +
                "CAST(shipdate AS VARCHAR) AS shipdate " +
                "FROM tpch.tiny.lineitem where orderkey between 20000 and 20010", 32);
    }

    protected void insertIntoOrders(long id)
    {
        assertUpdate("INSERT INTO %s VALUES (%s, 400, '1995-02-01', DOUBLE '99.99', 'N')".formatted(ordersTable, id), 1);
    }

    @AfterAll
    public void tearDown()
    {
        dropSourceTable(ordersTable);
        dropSourceTable(lineitemTable);
        dropSourceTable(nestedTypeTable);
        dropSourceTable(coercionTable);
    }

    /**
     * Optional {@code WITH (...)} clause (including a trailing space) applied when creating the
     * {@code orders} and {@code lineitem} source tables. Defaults to none; connectors that need
     * row-level {@code DELETE}/{@code INSERT} to age the MV during tests (e.g. Hive, which supports
     * these only on transactional tables) override this.
     */
    protected String sourceTablePropertiesClause()
    {
        return "";
    }

    /**
     * Whether the source connector supports row-level {@code DELETE} on the {@code orders} table.
     * The staleness tests age the MV by inserting then deleting a row; connectors that cannot delete
     * individual rows from the source (e.g. the great-lakes wrappers over Hive, which do not support
     * Hive ACID merge) override this to skip those tests.
     */
    protected boolean sourceSupportsRowLevelDelete()
    {
        return true;
    }

    /**
     * Name of the materialized-view table property that declares partitioning columns for the storage
     * table (e.g. {@code partitioning} for Iceberg, {@code partitioned_by} for Hive).
     */
    protected abstract String partitionedByPropertyName();

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
        String projectedColumns = columns.stream()
                .map(CoercionColumn::name)
                .collect(joining(", "));
        try (TestMaterializedView materializedView = createSubstitutionMv("mv_coercion_", "SELECT id_col, " + projectedColumns + " FROM " + coercionTable)) {
            for (CoercionColumn column : columns) {
                assertSubstituted("SELECT " + column.projection().formatted(column.name()) + " FROM " + coercionTable, coercionTable, materializedView.mvName());
            }
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

    protected void createCoercionTable()
    {
        List<CoercionColumn> columns = coercionColumns();
        String columnDefinitions = columns.stream()
                .map(column -> column.name() + " " + column.columnType())
                .collect(joining(", "));
        String values = columns.stream()
                .map(CoercionColumn::insertValue)
                .collect(joining(", "));
        sourceSqlExecutor().execute("CREATE TABLE %s (id_col BIGINT, %s)".formatted(sourceTableReference(coercionTable), columnDefinitions));
        sourceSqlExecutor().execute("INSERT INTO %s VALUES (1, %s)".formatted(sourceTableReference(coercionTable), values));
    }

    @Test
    public void testScanOnlySubstitution()
    {
        try (TestMaterializedView materializedView = createSubstitutionMv("mv_scan_", "SELECT * FROM " + ordersTable)) {
            assertSubstituted("SELECT * FROM " + ordersTable, ordersTable, materializedView.mvName());
        }
    }

    @Test
    public void testScanOnlySubstitutionWithColumnA()
    {
        try (TestMaterializedView materializedView = createSubstitutionMv("mv_col_subset_", "SELECT orderkey, custkey, totalprice FROM " + ordersTable)) {
            assertSubstituted("SELECT orderkey, totalprice FROM " + ordersTable, ordersTable, materializedView.mvName());
        }
    }

    @Test
    public void testScanOnlySubstitutionWithColumnAlias()
    {
        try (TestMaterializedView materializedView = createSubstitutionMv("mv_col_subset_", "SELECT orderkey as o_key, custkey as c_key, totalprice as t_price FROM " + ordersTable)) {
            assertSubstituted("SELECT orderkey, custkey, totalprice FROM " + ordersTable, ordersTable, materializedView.mvName());
        }
    }

    @Test
    public void testScanOnlySubstitutionColumnMissing()
    {
        try (var _ = createSubstitutionMv("mv_col_missing_", "SELECT orderkey, custkey FROM " + ordersTable)) {
            Session session = sessionWithSubstitution();
            // Query needs totalprice which MV does not have — should NOT substitute
            assertNotSubstituted(session, "SELECT orderkey, totalprice FROM " + ordersTable, ordersTable);
            assertSameResults(session, "SELECT orderkey, totalprice FROM " + ordersTable);
        }
    }

    @Test
    public void testScanSelectStarWithColumnSubsetMv()
    {
        // SELECT * leaves IcebergTableHandle.projectedColumns empty (PruneTableScanColumns
        // does not fire when every output is referenced), so canSubstitute cannot reject
        // on column coverage alone — the per-column null check in MatchingVisitor must.
        try (var _ = createSubstitutionMv("mv_col_missing_star_", "SELECT orderkey, custkey FROM " + ordersTable)) {
            Session session = sessionWithSubstitution();
            assertNotSubstituted(session, "SELECT * FROM " + ordersTable, ordersTable);
            assertSameResults(session, "SELECT * FROM " + ordersTable);
        }
    }

    @Test
    public void testScanWithSubFieldProjectionOverMv()
    {
        // The MV captures the whole struct/JSON column; the query projects individual
        // sub-fields. Substitution must fire because the TableScan sees the same base
        // column on both sides — the sub-field expression lives in a Project above the
        // scan and runs against the substituted MV storage.
        try (TestMaterializedView materializedView = createSubstitutionMv("mv_sub_field_", "SELECT id, info FROM " + nestedTypeTable)) {
            SubFieldTestContext context = subFieldTestContext();
            String nameQuery = "SELECT " + context.subFieldExpression("info", "name") + " FROM " + nestedTypeTable;
            assertSubstituted(nameQuery, nestedTypeTable, materializedView.mvName());

            String ageQuery = "SELECT " + context.subFieldExpression("info", "age") + " FROM " + nestedTypeTable;
            assertSubstituted(ageQuery, nestedTypeTable, materializedView.mvName());
        }
    }

    @Test
    public void testSubFieldMaterializingMvDoesNotSubstitute()
    {
        SubFieldTestContext context = subFieldTestContext();
        String definition = "SELECT id, " + context.subFieldExpression("info", "name") + " AS a FROM " + nestedTypeTable;
        // The MV materializes a single sub-field expression (info.name), not the whole struct, so its
        // defining query is not a bare table scan and cannot be indexed for substitution. Enabling
        // substitution is therefore rejected when the MV is created.
        // This will change to a plain create when projections are supported
        assertQueryFails(
                "CREATE MATERIALIZED VIEW %s WITH (substitution_enabled = true) AS %s".formatted(mvName("mv_sub_field_same_type_"), definition),
                ".*cannot be enabled for substitution: its definition query is not supported by materialized view substitution");

        try (var _ = createFreshMv("mv_sub_field_same_type_", definition)) {
            Session session = sessionWithSubstitution();
            String materializedFieldQuery = "SELECT " + context.subFieldExpression("info", "name") + " FROM " + nestedTypeTable;
            // This will change to assertSubstituted when projections are supported
            assertNotSubstituted(session, materializedFieldQuery, nestedTypeTable);
            assertSameResults(session, materializedFieldQuery);

            String siblingFieldQuery = "SELECT " + context.subFieldExpression("info", "gender") + " FROM " + nestedTypeTable;
            // We should not substitute a subfield if it is not materialized. This will test for invalid ConnectorColumnId implementations that only ontain the base column name,
            // once projection support is added
            assertNotSubstituted(session, siblingFieldQuery, nestedTypeTable);
            assertSameResults(session, siblingFieldQuery);
        }
    }

    protected void createNestedTypeTable(CatalogSchemaTableName table)
    {
        SubFieldTestContext context = subFieldTestContext();
        sourceSqlExecutor().execute("CREATE TABLE %s (id BIGINT, info %s)".formatted(sourceTableReference(table), context.subFieldColumnType()));
        sourceSqlExecutor().execute("INSERT INTO %s VALUES %s".formatted(sourceTableReference(table), context.subFieldInsertValues()));
    }

    @Test
    public void testScanWithWhereClauseOnSubstitutedScan()
    {
        try (TestMaterializedView materializedView = createSubstitutionMv("mv_scan_where_", "SELECT * FROM " + ordersTable)) {
            String query = "SELECT * FROM " + ordersTable + " WHERE orderstatus = 'F'";
            assertSubstituted(query, ordersTable, materializedView.mvName());
        }
    }

    @Test
    public void testScanWithAggregationOnSubstitutedScan()
    {
        try (TestMaterializedView materializedView = createSubstitutionMv("mv_scan_agg_", "SELECT * FROM " + ordersTable)) {
            String query = "SELECT count(*), sum(totalprice) FROM " + ordersTable + " WHERE orderdate > '1995-01-12'";
            assertSubstituted(query, ordersTable, materializedView.mvName());
        }
    }

    @Test
    public void testSubstitutionDisabledBySessionProperty()
    {
        try (var _ = createSubstitutionMv("mv_disabled_session_", "SELECT * FROM " + ordersTable)) {
            assertNotSubstituted(sessionWithoutSubstitution(), "SELECT * FROM " + ordersTable, ordersTable);
        }
    }

    @Test
    public void testSubstitutionEnabledBySessionProperty()
    {
        try (TestMaterializedView materializedView = createSubstitutionMv("mv_enabled_session_", "SELECT * FROM " + ordersTable)) {
            assertSubstituted("SELECT * FROM " + ordersTable, ordersTable, materializedView.mvName());
        }
    }

    @Test
    public void testStaleUnrefreshedMvNotSubstituted()
    {
        // Create but do NOT refresh
        try (var _ = createMvWithoutRefresh("mv_unrefreshed_", "SELECT * FROM " + ordersTable, SUBSTITUTION_ENABLED)) {
            assertNotSubstituted(sessionWithSubstitution(), "SELECT * FROM " + ordersTable, ordersTable);
        }
    }

    @Test
    public void testStaleMvAfterBaseTableInsert()
    {
        assumeTrue(sourceSupportsRowLevelDelete(), "source does not support row-level DELETE");
        try (TestMaterializedView materializedView = createSubstitutionMv("mv_stale_insert_", "SELECT * FROM " + ordersTable)) {
            // Make MV stale by inserting into base table
            insertIntoOrders(99990001);

            // We do not support detecting source table change, so the substitution relies on the grace-period, and stale MV will be used
            assertSubstituted(false, "SELECT * FROM " + ordersTable, ordersTable, materializedView.mvName());
        }
    }

    @Test
    public void testRefreshedMvBecomesSubstitutableAgain()
    {
        assumeTrue(sourceSupportsRowLevelDelete(), "source does not support row-level DELETE");
        long baseCount = (long) computeActual("SELECT count(*) FROM " + ordersTable).getOnlyValue();
        try (TestMaterializedView materializedView = createSubstitutionMv("mv_rerefresh_", "SELECT * FROM " + ordersTable, Duration.ofHours(1))) {
            insertIntoOrders(99990002);
            // Session start time past the grace period so the stale MV is not used.
            Session expiredSession = Session.builder(sessionWithSubstitution())
                    .setSystemProperty("session_start_time", Instant.now().plus(1, ChronoUnit.DAYS).toString())
                    .build();
            assertNotSubstituted(expiredSession, "SELECT * FROM " + ordersTable, ordersTable);

            // Re-refresh makes it fresh again
            getQueryRunner().execute("REFRESH MATERIALIZED VIEW " + materializedView.mvName());
            assertSubstituted("SELECT * FROM " + ordersTable, ordersTable, materializedView.mvName());

            // Result correctness after re-refresh
            assertThat(computeActual(sessionWithSubstitution(), "SELECT count(*) FROM " + ordersTable).getOnlyValue()).isEqualTo(baseCount + 1);
        }
    }

    @Test
    public void testMvWithGracePeriodUsedWhenWithinWindow()
    {
        assumeTrue(sourceSupportsRowLevelDelete(), "source does not support row-level DELETE");
        try (TestMaterializedView materializedView = createSubstitutionMv("mv_grace_within_", "SELECT * FROM " + ordersTable, Duration.ofHours(1))) {
            // Insert to make it technically stale
            insertIntoOrders(99990003);

            // Within grace period — should still substitute
            assertSubstituted(false, "SELECT * FROM " + ordersTable, ordersTable, materializedView.mvName());
        }
    }

    @Test
    public void testMvWithGracePeriodNotUsedWhenExpired()
    {
        assumeTrue(sourceSupportsRowLevelDelete(), "source does not support row-level DELETE");
        try (var _ = createSubstitutionMv("mv_grace_expired_", "SELECT * FROM " + ordersTable, Duration.ofSeconds(1))) {
            // Insert to make it stale
            insertIntoOrders(99990004);

            // Use a session with future start time to ensure grace period has expired
            Session futureSession = Session.builder(sessionWithSubstitution())
                    .setSystemProperty("session_start_time", Instant.now().plus(1, ChronoUnit.DAYS).toString())
                    .build();

            assertNotSubstituted(futureSession, "SELECT * FROM " + ordersTable, ordersTable);
        }
    }

    @Test
    public void testMvWithoutSubstitutionPropertyNotUsed()
    {
        // No substitution_enabled property — default is false
        try (var _ = createFreshMv("mv_no_prop_", "SELECT * FROM " + ordersTable)) {
            assertNotSubstituted(sessionWithSubstitution(), "SELECT * FROM " + ordersTable, ordersTable);
        }
    }

    @Test
    public void testMvWithSubstitutionDisabledNotUsed()
    {
        try (var _ = createFreshMv("mv_sub_false_", "SELECT * FROM " + ordersTable, SUBSTITUTION_DISABLED)) {
            assertNotSubstituted(sessionWithSubstitution(), "SELECT * FROM " + ordersTable, ordersTable);
        }
    }

    @Test
    public void testMvWithSubstitutionEnabledUsed()
    {
        try (TestMaterializedView materializedView = createSubstitutionMv("mv_sub_true_", "SELECT * FROM " + ordersTable)) {
            assertSubstituted("SELECT * FROM " + ordersTable, ordersTable, materializedView.mvName());
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
        // Create without refresh — storage table is empty.
        try (TestMaterializedView materializedView = createMvWithoutRefresh("mv_alter_no_refresh_", "SELECT * FROM " + ordersTable, ImmutableMap.of())) {
            // Flip substitution on. Currently this stamps Instant.now() as lastKnownFreshTime
            // and indexes the (empty) storage anyway.
            assertUpdate("ALTER MATERIALIZED VIEW " + materializedView.mvName() + " SET PROPERTIES substitution_enabled = true");

            Session session = sessionWithSubstitution();
            // Substitution must NOT fire — storage is empty and serving 0 rows would be wrong.
            assertNotSubstituted(session, "SELECT * FROM " + ordersTable, ordersTable);
            assertSameResults(session, "SELECT * FROM " + ordersTable);
        }
    }

    @Test
    public void testMvSubstitutionEnabledViaAlterSetProperties()
    {
        // MV is created and refreshed without substitution_enabled, so its definition is NOT
        // indexed in the MaterializationIndex. ALTER ... SET PROPERTIES then turns substitution
        // on; the subsequent REFRESH must observe the flipped property and index the
        // materialization so the optimizer can substitute.
        try (TestMaterializedView materializedView = createFreshMv("mv_alter_enable_", "SELECT * FROM " + ordersTable)) {
            Session session = sessionWithSubstitution();
            // Before ALTER: substitution_enabled is false (default), so no index entry exists.
            assertNotSubstituted(session, "SELECT * FROM " + ordersTable, ordersTable);

            // Flipping substitution_enabled via ALTER must take effect immediately — no extra
            // REFRESH should be required, because the storage table is already fresh.
            assertUpdate("ALTER MATERIALIZED VIEW " + materializedView.mvName() + " SET PROPERTIES substitution_enabled = true");

            assertSubstituted("SELECT * FROM " + ordersTable, ordersTable, materializedView.mvName());
        }
    }

    @Test
    public void testMultipleMvsOnSameTable()
    {
        try (TestMaterializedView mvFull = createSubstitutionMv("mv_full_", "SELECT * FROM " + ordersTable);
                TestMaterializedView mvPartial = createSubstitutionMv("mv_partial_", "SELECT orderkey, custkey FROM " + ordersTable)) {
            // Either MV could serve this query
            assertSubstituted("SELECT orderkey, custkey FROM " + ordersTable, ordersTable, ImmutableSet.of(mvFull.mvName(), mvPartial.mvName()));
        }
    }

    @Test
    @Disabled("We could make it work for iceberg MVs on iceberg source tables, but performance of checking MV freshness for every query mut be carefully considered")
    public void testStaleAndFreshMvPicksFresh()
    {
        assumeTrue(sourceSupportsRowLevelDelete(), "source does not support row-level DELETE");
        long baseCount = (long) computeActual("SELECT count(*) FROM " + ordersTable).getOnlyValue();
        try (var _ = createSubstitutionMv("mv_stale_", "SELECT * FROM " + ordersTable)) {
            // Make mvStale stale
            insertIntoOrders(99990005);

            // Create a fresh MV after the insert
            try (TestMaterializedView mvFresh = createSubstitutionMv("mv_fresh_", "SELECT * FROM " + ordersTable)) {
                Session session = sessionWithSubstitution();
                assertSubstituted("SELECT * FROM " + ordersTable, ordersTable, mvFresh.mvName());
                // Result should include the new row (from the fresh MV)
                assertThat(computeActual(session, "SELECT count(*) FROM " + ordersTable).getOnlyValue()).isEqualTo(baseCount + 1);
            }
        }
    }

    @Test
    public void testSubstitutedQueryReturnsCorrectResults()
    {
        try (var _ = createSubstitutionMv("mv_correctness_", "SELECT * FROM " + ordersTable)) {
            Session session = sessionWithSubstitution();

            assertSameResults(session, "SELECT * FROM " + ordersTable);
            assertSameResults(session, "SELECT * FROM " + ordersTable + " WHERE orderdate = '1995-01-15'");
            assertSameResults(session, "SELECT * FROM " + ordersTable + " ORDER BY orderkey");
            assertSameResults(session, "SELECT count(*) FROM " + ordersTable);
            assertSameResults(session, "SELECT custkey, sum(totalprice) FROM " + ordersTable + " GROUP BY custkey");
        }
    }

    @Test
    public void testSubstitutedQueryWithJoin()
    {
        try (TestMaterializedView materializedView = createSubstitutionMv("mv_join_", "SELECT * FROM " + ordersTable)) {
            String query = "SELECT o.orderkey, l.quantity " +
                    "FROM " + ordersTable + " o JOIN " + lineitemTable + " l ON o.orderkey = l.orderkey";

            // orders scan should be substituted, lineitem scan should not
            assertSubstituted(query, ordersTable, materializedView.mvName(), lineitemTable);
        }
    }

    @Test
    public void testRefreshReadsFromBaseTableNotMv()
    {
        assumeTrue(sourceSupportsRowLevelDelete(), "source does not support row-level DELETE");
        try (TestMaterializedView materializedView = createSubstitutionMv("mv_refresh_norecurse_", "SELECT * FROM " + ordersTable)) {
            // Change the base table so we can detect it was used during the refresh
            insertIntoOrders(99990006);

            // The refresh itself should read from base table, not from the MV
            getQueryRunner().execute(sessionWithSubstitution(), "REFRESH MATERIALIZED VIEW " + materializedView.mvName());

            // Verify mv is refreshed from the base table
            assertSameResults(getSession(), "SELECT * FROM " + materializedView.mvName(), "SELECT * FROM " + ordersTable);
        }
    }

    @Test
    public void testSubstitutionAfterAddColumnToBaseTable()
    {
        CatalogSchemaTableName tableName = sourceTable("orders_evolve_" + randomNameSuffix());
        try {
            sourceSqlExecutor().execute("CREATE TABLE %s AS SELECT orderkey, totalprice FROM %s".formatted(sourceTableReference(tableName), sourceTableReference(ordersTable)));
            try (TestMaterializedView materializedView = createSubstitutionMv("mv_evolve_", "SELECT orderkey, totalprice FROM " + tableName)) {
                sourceSqlExecutor().execute("ALTER TABLE %s ADD COLUMN new_col CHAR(1)".formatted(sourceTableReference(tableName)));

                // Query for original columns — MV should still be usable
                assertSubstituted("SELECT orderkey, totalprice FROM " + tableName, tableName, materializedView.mvName());
            }
        }
        finally {
            dropSourceTable(tableName);
        }
    }

    @Test
    public void testSubstitutionAfterDropMv()
    {
        CatalogSchemaTableName mvName = createSubstitutionMv("mv_dropped_", "SELECT * FROM " + ordersTable).mvName();
        try {
            assertSubstituted("SELECT * FROM " + ordersTable, ordersTable, mvName);

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
        assumeTrue(sourceSupportsRowLevelDelete(), "source does not support row-level DELETE");
        try (TestMaterializedView materializedView = createSubstitutionMv("mv_grace_removed_", "SELECT * FROM " + ordersTable, Duration.ofHours(1))) {
            insertIntoOrders(99990007);

            // Stale but within 1-hour grace period — substitution works
            assertSubstituted(false, "SELECT * FROM " + ordersTable, ordersTable, materializedView.mvName());

            // Replace MV without grace period (empty = Iceberg treats as infinite, so use zero)
            assertUpdate("CREATE OR REPLACE MATERIALIZED VIEW " + materializedView.mvName() +
                    " GRACE PERIOD INTERVAL '0' SECOND" +
                    " WITH (substitution_enabled = true)" +
                    " AS SELECT * FROM " + ordersTable);
            getQueryRunner().execute("REFRESH MATERIALIZED VIEW " + materializedView.mvName());

            insertIntoOrders(99990008);

            // Stale with zero grace period — substitution must not happen
            assertNotSubstituted(sessionWithSubstitution(), "SELECT * FROM " + ordersTable, ordersTable);
        }
    }

    @Test
    public void testSubstitutionStartsWhenGracePeriodAdded()
    {
        assumeTrue(sourceSupportsRowLevelDelete(), "source does not support row-level DELETE");
        long baseCount = (long) computeActual("SELECT count(*) FROM " + ordersTable).getOnlyValue();
        try (TestMaterializedView materializedView = createSubstitutionMv("mv_grace_added_", "SELECT * FROM " + ordersTable, Duration.ZERO)) {
            insertIntoOrders(99990009);

            // Stale with zero grace period — substitution does not work
            assertNotSubstituted(sessionWithSubstitution(), "SELECT * FROM " + ordersTable, ordersTable);

            // Replace MV with a generous grace period
            assertUpdate("CREATE OR REPLACE MATERIALIZED VIEW " + materializedView.mvName() +
                    " GRACE PERIOD INTERVAL '1' HOUR" +
                    " WITH (substitution_enabled = true)" +
                    " AS SELECT * FROM " + ordersTable);
            getQueryRunner().execute("REFRESH MATERIALIZED VIEW " + materializedView.mvName());

            insertIntoOrders(99990010);

            // Stale but within 1-hour grace period — substitution works now
            MaterializedResultWithPlan result = assertSubstituted(false, "SELECT count(*) FROM " + ordersTable, ordersTable, materializedView.mvName());
            // MV was refreshed after the first insert but before the second insert — stale data has baseCount+1
            assertThat(result.result().getOnlyValue())
                    .as("MV is stale but within grace period — should return MV row count, missing the latest insert")
                    .isEqualTo(baseCount + 1);
        }
    }

    @Test
    public void testSubstitutionAfterMvQueryTextChanged()
    {
        try (TestMaterializedView materializedView = createSubstitutionMv("mv_query_changed_", "SELECT * FROM " + ordersTable)) {
            assertSubstituted("SELECT * FROM " + ordersTable, ordersTable, materializedView.mvName());

            // Replace MV with a different query (different source table)
            assertUpdate("CREATE OR REPLACE MATERIALIZED VIEW " + materializedView.mvName() +
                    " WITH (substitution_enabled = true)" +
                    " AS SELECT * FROM " + lineitemTable);

            // Before refresh — MV is stale with the new definition, substitution should not work
            assertNotSubstituted(sessionWithSubstitution(), "SELECT * FROM " + lineitemTable, lineitemTable);
            // Old query should also not be substituted (definition no longer matches orders)
            assertNotSubstituted(sessionWithSubstitution(), "SELECT * FROM " + ordersTable, ordersTable);

            // Refresh with the new definition
            getQueryRunner().execute("REFRESH MATERIALIZED VIEW " + materializedView.mvName());

            // After refresh — substitution works for the new query
            assertSubstituted("SELECT * FROM " + lineitemTable, lineitemTable, materializedView.mvName());
            assertSameResults(sessionWithSubstitution(), "SELECT * FROM " + lineitemTable);
            // Old query still not substituted (MV no longer covers orders)
            assertNotSubstituted(sessionWithSubstitution(), "SELECT * FROM " + ordersTable, ordersTable);
        }
    }

    @Test
    public void testSubstitutionAfterRenameMv()
    {
        CatalogSchemaTableName renamedMvName = mvName("mv_rename_new_");
        CatalogSchemaTableName mvName = createSubstitutionMv("mv_rename_orig_", "SELECT * FROM " + ordersTable).mvName();
        try {
            assertSubstituted("SELECT * FROM " + ordersTable, ordersTable, mvName);

            assertUpdate("ALTER MATERIALIZED VIEW " + mvName + " RENAME TO " + renamedMvName);

            // Substitution should still work after rename
            assertSubstituted("SELECT * FROM " + ordersTable, ordersTable, renamedMvName);
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
        // First incarnation: covers orders
        CatalogSchemaTableName mvName = createSubstitutionMv("mv_recreate_", "SELECT * FROM " + ordersTable).mvName();
        try {
            assertSubstituted("SELECT * FROM " + ordersTable, ordersTable, mvName);

            assertUpdate("DROP MATERIALIZED VIEW " + mvName);

            // Second incarnation under the same name: covers lineitem (different base table).
            // Written out rather than via createSubstitutionMv, which would append a fresh name suffix.
            assertUpdate("CREATE MATERIALIZED VIEW " + mvName + " WITH (substitution_enabled = true) AS SELECT * FROM " + lineitemTable);
            getQueryRunner().execute("REFRESH MATERIALIZED VIEW " + mvName);

            // The old computation pattern must not be substituted by the new MV's storage
            assertNotSubstituted(sessionWithSubstitution(), "SELECT * FROM " + ordersTable, ordersTable);
            // The new pattern is served by the new MV
            assertSubstituted("SELECT * FROM " + lineitemTable, lineitemTable, mvName);
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
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .putAll(SUBSTITUTION_ENABLED)
                .put(partitionedByPropertyName(), "ARRAY['orderstatus']")
                .buildOrThrow();
        try (TestMaterializedView materializedView = createFreshMv("mv_partitioned_", "SELECT * FROM " + ordersTable, properties)) {
            assertSubstituted("SELECT * FROM " + ordersTable + " WHERE orderstatus = 'F'", ordersTable, materializedView.mvName());
        }
    }

    @Test
    public void testNoSubstitutionForDifferentTable()
    {
        try (var _ = createSubstitutionMv("mv_wrong_table_", "SELECT * FROM " + ordersTable)) {
            // Query on lineitem should NOT be substituted by an orders MV
            assertNotSubstituted(sessionWithSubstitution(), "SELECT * FROM " + lineitemTable, lineitemTable);
        }
    }

    @Test
    public void testGracePeriodMvReturnsStaleData()
    {
        assumeTrue(sourceSupportsRowLevelDelete(), "source does not support row-level DELETE");
        long baseCount = (long) computeActual("SELECT count(*) FROM " + ordersTable).getOnlyValue();
        try (TestMaterializedView materializedView = createSubstitutionMv("mv_stale_data_", "SELECT * FROM " + ordersTable, Duration.ofHours(1))) {
            // MV has baseCount rows. Insert one more row into the base table.
            insertIntoOrders(99990011);

            // Within grace period: substitution uses MV, which does NOT have the new row
            MaterializedResultWithPlan result = assertSubstituted(false, "SELECT count(*) FROM " + ordersTable, ordersTable, materializedView.mvName());
            assertThat(result.result().getOnlyValue())
                    .as("MV is stale but within grace period — should return MV data, not base table data")
                    .isEqualTo(baseCount);
        }
    }

    @Test
    public void testMvProjectionExactMatchSubstitutes()
    {
        // MV projects exactly the columns the query needs
        try (TestMaterializedView materializedView = createSubstitutionMv("mv_proj_exact_", "SELECT orderkey, orderstatus, totalprice FROM " + ordersTable)) {
            assertSubstituted("SELECT orderkey, orderstatus, totalprice FROM " + ordersTable, ordersTable, materializedView.mvName());
        }
    }

    @Test
    public void testMvProjectionSupersetSubstitutes()
    {
        // MV has more columns than the query needs — query can still use MV
        try (TestMaterializedView materializedView = createSubstitutionMv("mv_proj_superset_", "SELECT orderkey, custkey, orderstatus, totalprice FROM " + ordersTable)) {
            assertSubstituted("SELECT orderkey, totalprice FROM " + ordersTable, ordersTable, materializedView.mvName());
        }
    }

    @Test
    public void testMvProjectionMissingColumnDoesNotSubstitute()
    {
        // MV lacks a column the query needs — cannot substitute
        try (var _ = createSubstitutionMv("mv_proj_missing_", "SELECT orderkey, custkey FROM " + ordersTable)) {
            Session session = sessionWithSubstitution();
            String query = "SELECT orderkey, orderstatus FROM " + ordersTable;
            assertNotSubstituted(session, query, ordersTable);
        }
    }

    @Test
    public void testSubstitutionWorksWithFullAccess()
    {
        try (TestMaterializedView materializedView = createSubstitutionMv("mv_acl_ok_", "SELECT * FROM " + ordersTable)) {
            // User has SELECT on both base table and MV storage table — substitution works
            assertSubstituted("SELECT * FROM " + ordersTable, ordersTable, materializedView.mvName());
        }
    }

    @Test
    public void testSubstitutionFallsBackWhenMvAccessDenied()
    {
        // The user can read the base table but is denied access to the MV marked for substitution.
        // Substitution must not silently redirect the scan to the MV's storage table; it should fall
        // back to scanning the base table, which stays correct because base-table access is allowed.
        try (TestMaterializedView materializedView = createSubstitutionMv("mv_acl_mv_denied_", "SELECT * FROM " + ordersTable)) {
            getQueryRunner().getAccessControl().deny(privilege(materializedView.mvName().getSchemaTableName().getTableName(), SELECT_COLUMN));
            try {
                assertNotSubstituted(sessionWithSubstitution(), "SELECT * FROM " + ordersTable, ordersTable);
            }
            finally {
                getQueryRunner().getAccessControl().reset();
            }
        }
    }

    @Test
    public void testSubstitutionRespectsColumnLevelMvAccess()
    {
        // The user has SELECT on some MV columns but not others (here totalprice is denied).
        // Substitution applies only to queries that read the allowed columns; a query that reads
        // the denied column falls back to the base table (which the user can read in full).
        try (TestMaterializedView materializedView = createSubstitutionMv("mv_acl_col_", "SELECT * FROM " + ordersTable)) {
            getQueryRunner().getAccessControl().deny(privilege(materializedView.mvName().getSchemaTableName().getTableName() + ".totalprice", SELECT_COLUMN));
            try {
                // Reads only allowed columns — substituted
                assertSubstituted("SELECT orderkey, custkey FROM " + ordersTable, ordersTable, materializedView.mvName());
                // Reads the denied column — falls back to the base table
                assertNotSubstituted(sessionWithSubstitution(), "SELECT totalprice FROM " + ordersTable, ordersTable);
            }
            finally {
                getQueryRunner().getAccessControl().reset();
            }
        }
    }

    @Test
    public void testMvNotMarkedForSubstitutionIsIgnored()
    {
        long baseCount = (long) computeActual("SELECT count(*) FROM " + ordersTable).getOnlyValue();
        // Create MV WITHOUT substitution_enabled — default is false
        try (var _ = createFreshMv("mv_not_marked_", "SELECT * FROM " + ordersTable)) {
            // Even with session property enabled, this MV should not be used
            assertNotSubstituted(sessionWithSubstitution(), "SELECT * FROM " + ordersTable, ordersTable);

            // Verify the query still returns correct data from the base table
            assertThat(computeActual(sessionWithSubstitution(), "SELECT count(*) FROM " + ordersTable).getOnlyValue()).isEqualTo(baseCount);
        }
    }

    @Test
    public void testMvExplicitlyDisabledForSubstitutionIsIgnored()
    {
        try (var _ = createFreshMv("mv_disabled_", "SELECT * FROM " + ordersTable, SUBSTITUTION_DISABLED)) {
            assertNotSubstituted(sessionWithSubstitution(), "SELECT * FROM " + ordersTable, ordersTable);
        }
    }

    @Test
    public void testMvMarkedForSubstitutionCoexistsWithUnmarked()
    {
        // Two MVs exist: one marked, one not. Only the marked one should be used.
        try (TestMaterializedView mvMarked = createSubstitutionMv("mv_marked_", "SELECT * FROM " + ordersTable);
                var _ = createFreshMv("mv_unmarked_", "SELECT * FROM " + ordersTable)) {
            assertSubstituted("SELECT * FROM " + ordersTable, ordersTable, mvMarked.mvName());
        }
    }

    protected SubFieldTestContext subFieldTestContext()
    {
        return SubFieldTestContext.VARCHAR_JSON;
    }

    public abstract static class SubFieldTestContext
    {
        // Type declaration for a column that exposes named sub-fields,
        private final String subFieldColumnType;
        // INSERT values for {@link #subFieldColumnType()}: three rows, each with id BIGINT
        // and an info column carrying sub-fields {@code name} (VARCHAR) and {@code age} (INTEGER).
        private final String subFieldInsertValues;

        public static final SubFieldTestContext ROW = new SubFieldTestContext(
                "ROW(name VARCHAR, age INTEGER, gender VARCHAR)",
                """
                (1, CAST(ROW('Alice', 30, 'W') AS ROW(name VARCHAR, age INTEGER, gender VARCHAR))),
                (2, CAST(ROW('Bob', 25, 'M') AS ROW(name VARCHAR, age INTEGER, gender VARCHAR))),
                (3, CAST(ROW('Carol', 40, 'W') AS ROW(name VARCHAR, age INTEGER, gender VARCHAR)))
                """)
        {
            @Override
            protected String subFieldExpression(String column, String field)
            {
                return column + "." + field;
            }
        };

        public static final String VARCHAR_JSON_VALUES =
                """
                (1, '{"name":"Alice","age":30, "gender":"W"}'),
                (2, '{"name":"Bob","age":25, "gender":"M"}'),
                (3, '{"name":"Carol","age":40, "gender":"W"}')
                """;

        public static final SubFieldTestContext VARCHAR_JSON = new SubFieldTestContext("VARCHAR", VARCHAR_JSON_VALUES)
        {
            @Override
            protected String subFieldExpression(String column, String field)
            {
                return "json_extract_scalar(" + column + ", '$." + field + "')";
            }
        };

        public static final SubFieldTestContext JSON = new SubFieldTestContext("JSON",
                """
                (1, JSON '{"name":"Alice","age":30, "gender":"W"}'),
                (2, JSON '{"name":"Bob","age":25, "gender":"M"}'),
                (3, JSON '{"name":"Carol","age":40, "gender":"W"}')
                """)
        {
            @Override
            protected String subFieldExpression(String column, String field)
            {
                return "json_extract_scalar(" + column + ", '$." + field + "')";
            }
        };

        protected SubFieldTestContext(String subFieldColumnType, String subFieldInsertValues)
        {
            this.subFieldColumnType = requireNonNull(subFieldColumnType, "subFieldColumnType is null");
            this.subFieldInsertValues = requireNonNull(subFieldInsertValues, "subFieldInsertValues is null");
        }

        public String subFieldColumnType()
        {
            return subFieldColumnType;
        }

        public String subFieldInsertValues()
        {
            return subFieldInsertValues;
        }

        /**
         * Expression returning a sub-field of {@code column} as a string. Defaults to a Trino
         * struct dereference; JSON-column connectors override to {@code json_extract_scalar}.
         */
        protected abstract String subFieldExpression(String column, String field);
    }
}
