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
package io.trino.tests.substitution;

import com.google.inject.Key;
import io.starburst.materialization.ir.Operation;
import io.starburst.materialization.ir.Output;
import io.starburst.materialization.ir.TableScan;
import io.starburst.materialization.metastore.MaterializationDefinition;
import io.starburst.materialization.metastore.MaterializationMetastore;
import io.starburst.materialization.metastore.MaterializationSource.MaterializedViewSource;
import io.starburst.server.substitution.SubstitutionMetadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.TableHandle;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.substitution.ConnectorTableId;
import io.trino.testing.AbstractTestQueryFramework;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Optional;

import static io.trino.plugin.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Validates engine <-> MaterializationMetastore lifecycle interactions for
 * substitution-enabled materialized views: create, refresh, CREATE OR REPLACE,
 * drop, and rename. Subclasses differ only in the source connector that hosts
 * the {@code orders} table.
 */
public abstract class AbstractIcebergMvMetastoreLifecycleTest
        extends AbstractTestQueryFramework
{
    /**
     * Catalog/schema where MVs are created (always iceberg.tpch).
     */
    protected CatalogSchemaName getMvCatalogSchema()
    {
        return new CatalogSchemaName(ICEBERG_CATALOG, "tpch");
    }

    /**
     * Catalog/schema where the {@code orders} source table lives.
     */
    protected abstract CatalogSchemaName getSourceCatalogSchema();

    @BeforeAll
    public void setUp()
    {
        assertUpdate("CREATE TABLE orders(" +
                "order_key BIGINT, " +
                "customer_key BIGINT, " +
                "order_date DATE, " +
                "total_price DECIMAL(12, 2), " +
                "status VARCHAR)");
        assertUpdate("INSERT INTO orders VALUES " +
                "(1, 100, DATE '2024-01-10', DECIMAL '150.00', 'SHIPPED'), " +
                "(2, 100, DATE '2024-01-10', DECIMAL '200.50', 'PENDING'), " +
                "(3, 200, DATE '2024-01-15', DECIMAL '75.25', 'SHIPPED'), " +
                "(4, 200, DATE '2024-01-15', DECIMAL '320.00', 'DELIVERED'), " +
                "(5, 300, DATE '2024-01-20', DECIMAL '50.00', 'SHIPPED'), " +
                "(6, 300, DATE '2024-01-20', DECIMAL '410.75', 'PENDING')", 6);
    }

    @Test
    public void testCreateWithSubstitutionEnabled()
    {
        CatalogSchemaTableName mv = createMv("mv_create_enabled_", "SELECT * FROM orders", true);
        try {
            assertEntryAbsent(mv);
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + mv);
        }
    }

    @Test
    public void testCreateWithSubstitutionDisabled()
    {
        CatalogSchemaTableName mv = createMv("mv_create_disabled_", "SELECT * FROM orders", false);
        try {
            assertEntryAbsent(mv);
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + mv);
        }
    }

    @Test
    public void testRefreshUpdatesLastKnownFreshTime()
    {
        CatalogSchemaTableName mv = createMv("mv_refresh_fresh_", "SELECT * FROM orders", true);
        try {
            assertEntryAbsent(mv);

            Instant t0 = Instant.now();
            getQueryRunner().execute("REFRESH MATERIALIZED VIEW " + mv);

            MaterializationDefinition refreshed = assertEntryPresent(mv);
            assertThat(refreshed.lastKnownFreshTime())
                    .as("lastKnownFreshTime should be at or after refresh start")
                    .isAfterOrEqualTo(t0);
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + mv);
        }
    }

    @Test
    public void testRefreshOfNonRegisteredMvLeavesMetastoreEmpty()
    {
        CatalogSchemaTableName mv = createMv("mv_refresh_unregistered_", "SELECT * FROM orders", false);
        try {
            assertEntryAbsent(mv);
            getQueryRunner().execute("REFRESH MATERIALIZED VIEW " + mv);
            assertEntryAbsent(mv);
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + mv);
        }
    }

    @Test
    public void testReplaceWithSameDefinitionDeletesEntry()
    {
        CatalogSchemaTableName mv = createMv("mv_replace_same_", "SELECT * FROM orders", true);
        try {
            getQueryRunner().execute("REFRESH MATERIALIZED VIEW " + mv);

            assertUpdate("CREATE OR REPLACE MATERIALIZED VIEW " + mv +
                    " WITH (substitution_enabled = true) AS SELECT * FROM orders");

            assertEntryAbsent(mv);
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + mv);
        }
    }

    @Test
    public void testReplaceWithDifferentDefinitionUpdatesIr()
    {
        CatalogSchemaTableName mv = createMv("mv_replace_diff_", "SELECT order_key FROM orders", true);
        try {
            getQueryRunner().execute("REFRESH MATERIALIZED VIEW " + mv);
            assertThat(assertEntryPresent(mv).computationPlanRoot().outputs())
                    .hasSize(1);

            assertUpdate("CREATE OR REPLACE MATERIALIZED VIEW " + mv +
                    " WITH (substitution_enabled = true) AS SELECT order_key, customer_key FROM orders");

            getQueryRunner().execute("REFRESH MATERIALIZED VIEW " + mv);

            MaterializationDefinition replaced = assertEntryPresent(mv);
            assertThat(replaced.computationPlanRoot().outputs())
                    .as("REPLACE should update IR to reflect new column shape")
                    .hasSize(2);
            assertThat(replaced.computationPlanRoot().columnNames())
                    .containsExactly("order_key", "customer_key");
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + mv);
        }
    }

    @Test
    public void testReplaceTogglesSubstitutionFalseToTrue()
    {
        CatalogSchemaTableName mv = createMv("mv_replace_off_to_on_", "SELECT * FROM orders", false);
        try {
            assertEntryAbsent(mv);

            assertUpdate("CREATE OR REPLACE MATERIALIZED VIEW " + mv +
                    " WITH (substitution_enabled = true) AS SELECT * FROM orders");

            getQueryRunner().execute("REFRESH MATERIALIZED VIEW " + mv);
            assertEntryPresent(mv);
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + mv);
        }
    }

    @Test
    public void testReplaceTogglesSubstitutionTrueToFalse()
    {
        CatalogSchemaTableName mv = createMv("mv_replace_on_to_off_", "SELECT * FROM orders", true);
        try {
            assertEntryAbsent(mv);
            getQueryRunner().execute("REFRESH MATERIALIZED VIEW " + mv);
            assertEntryPresent(mv);

            assertUpdate("CREATE OR REPLACE MATERIALIZED VIEW " + mv +
                    " WITH (substitution_enabled = false) AS SELECT * FROM orders");

            assertEntryAbsent(mv);
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + mv);
        }
    }

    @Test
    public void testDropRemovesEntry()
    {
        CatalogSchemaTableName mv = createMv("mv_drop_registered_", "SELECT * FROM orders", true);
        assertEntryAbsent(mv);
        getQueryRunner().execute("REFRESH MATERIALIZED VIEW " + mv);
        assertEntryPresent(mv);

        assertUpdate("DROP MATERIALIZED VIEW " + mv);

        assertEntryAbsent(mv);
    }

    @Test
    public void testDropOfNonRegisteredMvIsNoOp()
    {
        CatalogSchemaTableName mv = createMv("mv_drop_unregistered_", "SELECT * FROM orders", false);
        assertEntryAbsent(mv);

        assertUpdate("DROP MATERIALIZED VIEW " + mv);

        assertEntryAbsent(mv);
    }

    @Test
    public void testRenameRelocatesEntry()
    {
        CatalogSchemaTableName mvA = createMv("mv_rename_src_", "SELECT * FROM orders", true);
        CatalogSchemaName mvSchema = getMvCatalogSchema();
        CatalogSchemaTableName mvB = new CatalogSchemaTableName(
                mvSchema.getCatalogName(),
                mvSchema.getSchemaName(),
                "mv_rename_dst_" + randomNameSuffix());
        try {
            getQueryRunner().execute("REFRESH MATERIALIZED VIEW " + mvA);
            Instant freshTimeBefore = assertEntryPresent(mvA).lastKnownFreshTime();

            assertUpdate("ALTER MATERIALIZED VIEW " + mvA + " RENAME TO " + mvB);

            assertEntryAbsent(mvA);
            MaterializationDefinition renamed = assertEntryPresent(mvB);
            assertThat(mvSourceName(renamed))
                    .as("rename should update the MaterializedViewSource name")
                    .isEqualTo(mvB);
            assertThat(renamed.lastKnownFreshTime())
                    .as("rename should preserve lastKnownFreshTime")
                    .isEqualTo(freshTimeBefore);
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + mvA);
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + mvB);
        }
    }

    @Test
    public void testRenameOfNonRegisteredMvIsNoOp()
    {
        CatalogSchemaTableName mvA = createMv("mv_rename_unreg_src_", "SELECT * FROM orders", false);
        CatalogSchemaName mvSchema = getMvCatalogSchema();
        CatalogSchemaTableName mvB = new CatalogSchemaTableName(
                mvSchema.getCatalogName(),
                mvSchema.getSchemaName(),
                "mv_rename_unreg_dst_" + randomNameSuffix());
        try {
            assertEntryAbsent(mvA);

            assertUpdate("ALTER MATERIALIZED VIEW " + mvA + " RENAME TO " + mvB);

            assertEntryAbsent(mvA);
            assertEntryAbsent(mvB);
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + mvA);
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + mvB);
        }
    }

    private MaterializationMetastore metastore()
    {
        return getDistributedQueryRunner().getCoordinator()
                .getInstance(Key.get(MaterializationMetastore.class));
    }

    private CatalogSchemaTableName qualifiedMvName(String prefix)
    {
        CatalogSchemaName mvSchema = getMvCatalogSchema();
        return new CatalogSchemaTableName(
                mvSchema.getCatalogName(),
                mvSchema.getSchemaName(),
                prefix + randomNameSuffix());
    }

    /**
     * Issues {@code CREATE MATERIALIZED VIEW <mv> WITH (substitution_enabled = ?) AS <body>}
     * and returns the fully-qualified MV name. Does not refresh.
     */
    private CatalogSchemaTableName createMv(String prefix, String body, boolean substitutionEnabled)
    {
        CatalogSchemaTableName mv = qualifiedMvName(prefix);
        assertUpdate("CREATE MATERIALIZED VIEW %s WITH (substitution_enabled = %s) AS %s"
                .formatted(mv, substitutionEnabled, body));
        return mv;
    }

    private MaterializationDefinition assertEntryPresent(CatalogSchemaTableName mv)
    {
        Optional<MaterializationDefinition> definition = getDefinition(mv);
        assertThat(definition)
                .as("Expected metastore entry for %s", mv)
                .isPresent();
        return definition.orElseThrow();
    }

    private Optional<MaterializationDefinition> getDefinition(CatalogSchemaTableName mv)
    {
        return metastore().listMaterializations().stream()
                .filter(materialization -> materialization.source() instanceof MaterializedViewSource mvSource && mvSource.materializedViewName().equals(mv))
                .findAny();
    }

    private void assertEntryAbsent(CatalogSchemaTableName mv)
    {
        assertThat(getDefinition(mv))
                .as("Expected no metastore entry for %s", mv)
                .isEmpty();
    }

    /**
     * Light IR check: walk {@link Output} → source operation, find the
     * {@link TableScan}(s), and assert each one references the expected source
     * table by {@link ConnectorTableId}. Equality works because the
     * connector implementations ({@code IcebergTableId},
     * {@code JdbcTableId}) are records.
     */
    private void assertSourceTableScans(MaterializationDefinition def, CatalogSchemaTableName expectedSourceTable, int expectedScanCount)
    {
        Output output = def.computationPlanRoot();
        // Today the IR extractor produces Output -> TableScan only. If that
        // changes, this helper will need a recursive walk.
        Operation source = output.source();
        assertThat(source)
                .as("Expected the IR's Output.source() to be a TableScan")
                .isInstanceOf(TableScan.class);
        TableScan scan = (TableScan) source;
        assertThat(expectedScanCount)
                .as("This helper currently supports a single TableScan; got %s expected", expectedScanCount)
                .isEqualTo(1);

        ConnectorTableId expectedId = getExpectedTableId(expectedSourceTable);
        assertThat(scan.table().hash())
                .as("TableScan should reference %s (hash mismatch — likely wrong source table)", expectedSourceTable)
                .isEqualTo(expectedId.hash());
    }

    private ConnectorTableId getExpectedTableId(CatalogSchemaTableName table)
    {
        QualifiedObjectName name = new QualifiedObjectName(
                table.getCatalogName(),
                table.getSchemaTableName().getSchemaName(),
                table.getSchemaTableName().getTableName());
        return newTransaction().singleStatement().execute(getSession(), session -> {
            // Force-register the catalog with the transaction.
            getQueryRunner().getPlannerContext().getMetadata()
                    .getCatalogHandle(session, table.getCatalogName());
            TableHandle handle = getQueryRunner().getPlannerContext().getMetadata()
                    .getTableHandle(session, name)
                    .orElseThrow(() -> new IllegalStateException("Table not found: " + name));
            return getDistributedQueryRunner().getCoordinator()
                    .getInstance(Key.get(SubstitutionMetadata.class))
                    .getTableId(session, handle)
                    .orElseThrow(() -> new IllegalStateException("Source connector returned no identity for " + name))
                    .connectorId();
        });
    }

    private CatalogSchemaTableName sourceTable(String tableName)
    {
        CatalogSchemaName source = getSourceCatalogSchema();
        return new CatalogSchemaTableName(source.getCatalogName(), source.getSchemaName(), tableName);
    }

    private static CatalogSchemaTableName mvSourceName(MaterializationDefinition def)
    {
        return ((MaterializedViewSource) def.source())
                .materializedViewName();
    }
}
