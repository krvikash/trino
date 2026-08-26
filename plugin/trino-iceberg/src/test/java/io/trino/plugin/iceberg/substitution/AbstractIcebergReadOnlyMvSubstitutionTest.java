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

import io.trino.plugin.hive.substitution.AbstractMvSubstitutionTest;
import io.trino.plugin.iceberg.TestingIcebergPlugin;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

import java.nio.file.Path;
import java.util.Map;

import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.plugin.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

/**
 * End-to-end MV substitution test for a <em>read-only, fixed-schema</em> source connector.
 * <p>
 * Unlike {@link AbstractIcebergMvSubstitutionTest}, this suite never creates, populates, alters or
 * drops source tables — it only builds Iceberg-backed materialized views over pre-existing source
 * tables and asserts that base-table queries are redirected to the MV storage table. This fits
 * connectors whose test backend serves a fixed set of tables with no write path.
 * <p>
 * A subclass supplies a running source (see {@link #createSourceQueryRunner()}) and names:
 * <ul>
 *     <li>{@link #substitutedTable()} — the table whose scans should be substituted;</li>
 *     <li>{@link #otherTable()} — a different source table, used to prove a query on it is not
 *     substituted by the {@code substitutedTable()} MV;</li>
 *     <li>{@link #materializedColumn()} — a column of {@code substitutedTable()} that the MV
 *     materializes and the query reads;</li>
 *     <li>{@link #nonMaterializedColumn()} — another column of {@code substitutedTable()} that the
 *     MV does NOT materialize, used to prove a query reading it is not substituted.</li>
 * </ul>
 */
@Execution(SAME_THREAD)
public abstract class AbstractIcebergReadOnlyMvSubstitutionTest
        extends AbstractMvSubstitutionTest
{
    /**
     * Builds a query runner with the source connector's catalog already installed and the coordinator
     * property {@code materialized-view-substitution.support.enabled=true} set. The base class installs
     * the Iceberg catalog (used for MV storage) on top.
     */
    protected abstract QueryRunner createSourceQueryRunner()
            throws Exception;

    /**
     * A pre-existing source table whose scans should be substituted. Must expose at least
     * {@link #materializedColumn()} and {@link #nonMaterializedColumn()}.
     */
    protected abstract CatalogSchemaTableName substitutedTable();

    /**
     * A different pre-existing source table, used to prove a query on it is not substituted by an MV
     * defined over {@link #substitutedTable()}.
     */
    protected abstract CatalogSchemaTableName otherTable();

    /**
     * A column of {@link #substitutedTable()} materialized by the MV and read by the substituted query.
     */
    protected abstract String materializedColumn();

    /**
     * A column of {@link #substitutedTable()} the MV does NOT materialize; a query reading it must not
     * be substituted.
     */
    protected abstract String nonMaterializedColumn();

    @Override
    protected final QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = createSourceQueryRunner();
        try {
            Path baseDataDir = queryRunner.getCoordinator().getBaseDataDir();
            queryRunner.installPlugin(new TestingIcebergPlugin(baseDataDir));
            queryRunner.createCatalog(ICEBERG_CATALOG, "iceberg", Map.of(
                    "iceberg.catalog.type", "TESTING_FILE_METASTORE",
                    "hive.metastore.catalog.dir", "local:///iceberg-catalog",
                    "iceberg.hive-catalog-name", "hive"));
            queryRunner.execute("CREATE SCHEMA %s.tpch".formatted(ICEBERG_CATALOG));
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
        return queryRunner;
    }

    @Override
    protected CatalogSchemaName mvSchema()
    {
        return new CatalogSchemaName(ICEBERG_CATALOG, "tpch");
    }

    @Test
    public void testTableScanSubstituted()
    {
        CatalogSchemaTableName mvName = mvName("mv_scan_");
        try {
            createSubstitutionMv(mvName, "SELECT " + materializedColumn() + " FROM " + substitutedTable());

            assertSubstituted("SELECT " + materializedColumn() + " FROM " + substitutedTable(), substitutedTable(), mvName);
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + mvName);
        }
    }

    @Test
    public void testMaterializedColumnSubstituted()
    {
        CatalogSchemaTableName mvName = mvName("mv_column_");
        try {
            createSubstitutionMv(mvName, "SELECT " + materializedColumn() + " FROM " + substitutedTable());

            assertSubstituted("SELECT " + materializedColumn() + " FROM " + substitutedTable(), substitutedTable(), mvName);
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + mvName);
        }
    }

    @Test
    public void testNonMaterializedColumnNotSubstituted()
    {
        CatalogSchemaTableName mvName = mvName("mv_column_missing_");
        try {
            createSubstitutionMv(mvName, "SELECT " + materializedColumn() + " FROM " + substitutedTable());

            // The query needs a column the MV does not materialize — no substitution.
            assertNotSubstituted(sessionWithSubstitution(), "SELECT " + nonMaterializedColumn() + " FROM " + substitutedTable(), substitutedTable());
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + mvName);
        }
    }

    @Test
    public void testFilteredScanSubstituted()
    {
        CatalogSchemaTableName mvName = mvName("mv_scan_where_");
        try {
            createSubstitutionMv(mvName, "SELECT " + materializedColumn() + " FROM " + substitutedTable());

            // The base scan is a plain scan when substitution runs; the WHERE filter applies on top of
            // the substituted MV storage table.
            assertSubstituted("SELECT " + materializedColumn() + " FROM " + substitutedTable() + " WHERE " + materializedColumn() + " IS NOT NULL", substitutedTable(), mvName);
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + mvName);
        }
    }

    @Test
    public void testDifferentTableNotSubstituted()
    {
        CatalogSchemaTableName mvName = mvName("mv_wrong_table_");
        try {
            createSubstitutionMv(mvName, "SELECT " + materializedColumn() + " FROM " + substitutedTable());

            // A query on a different table must not be substituted by the substitutedTable() MV.
            // count(*) avoids assuming otherTable() shares any column name with substitutedTable().
            assertNotSubstituted(sessionWithSubstitution(), "SELECT count(*) FROM " + otherTable(), otherTable());
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + mvName);
        }
    }
}
