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
package com.starburstdata.plugin.kdb.substitution;

import com.starburstdata.plugin.kdb.KdbClient;
import com.starburstdata.plugin.kdb.KdbContainer;
import com.starburstdata.plugin.kdb.KdbQueryRunner;
import io.trino.Session;
import io.trino.plugin.iceberg.substitution.AbstractIcebergMvSubstitutionTest;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.SqlExecutor;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

/**
 * End-to-end MV substitution test with kdb+ as the source. The kdb+ connector is read-only, so
 * unlike the JDBC-backed sources the base tables cannot be created, populated or dropped through
 * Trino (or a backend SQL executor); they are seeded directly as kdb+ tables (q variables) through
 * the kdb+ client. The test then builds an Iceberg-backed materialized view over such a table and
 * asserts that base-table queries are redirected to the MV storage table.
 */
@Execution(SAME_THREAD)
public class TestIcebergMvOnKdbSourceSubstitution
        extends AbstractIcebergMvSubstitutionTest
{
    private KdbClient kdbClient;

    @Override
    protected QueryRunner createSourceQueryRunner(Session defaultSession, CatalogSchemaName sourceSchema)
            throws Exception
    {
        KdbContainer server = closeAfterClass(new KdbContainer());
        kdbClient = server.client();
        return KdbQueryRunner.builder(server)
                .addExtraProperty("materialized-view-substitution.support.enabled", "true")
                .build();
    }

    @Override
    protected CatalogSchemaName sourceSchema()
    {
        // "default" is the kdb+ root namespace, the connector's default schema (see KdbQueryRunner).
        return new CatalogSchemaName("kdb", "default");
    }

    // KdbQueryRunner already provisions the tpch catalog, and the kdb+ "default" schema is the root
    // namespace (tables are discovered dynamically), so no Trino-side source schema is created.
    @Override
    protected boolean addTpchConnector()
    {
        return false;
    }

    @Override
    protected boolean createSourceSchema()
    {
        return false;
    }

    @Override
    protected void createOrdersTable()
    {
        // orderstatus is a simple char column (one char per row); orderdate is a list of char
        // vectors (strings). Both map to Trino VARCHAR.
        kdbClient.execute(("%s:([]" +
                "orderkey:20000 20001 20002 20003 20004 20005 20006 20007j; " +
                "custkey:100 101 102 103 104 105 106 107j; " +
                "orderdate:(\"1995-01-05\";\"1995-01-10\";\"1995-01-15\";\"1995-01-15\";\"1995-01-20\";\"1995-01-25\";\"1995-01-30\";\"1995-02-01\"); " +
                "totalprice:100 200 300 400 500 600 700 800f; " +
                "orderstatus:\"OFPFOFPO\")")
                .formatted(tableName(ordersTable)));
    }

    @Override
    protected void createLineItemTable()
    {
        kdbClient.execute(("%s:([]" +
                "orderkey:20000 20000 20001 20002j; " +
                "linenumber:1 2 1 1j; " +
                "quantity:10 5 7 3f; " +
                "extendedprice:1000 500 700 300f; " +
                "shipdate:(\"1995-02-01\";\"1995-02-05\";\"1995-02-10\";\"1995-02-15\"))")
                .formatted(tableName(lineitemTable)));
    }

    @Override
    protected void insertIntoOrders(long id)
    {
        // `t upsert (...) amends the global table in place.
        kdbClient.execute("`%s upsert (%sj; 400j; \"1995-02-01\"; 99.99; \"N\")".formatted(tableName(ordersTable), id));
    }

    @Override
    protected void createNestedTypeTable(CatalogSchemaTableName table)
    {
        // kdb+ has no native struct/JSON type; the sub-field column holds JSON as VARCHAR (see
        // subFieldTestContext()). Each info value is a list of chars (a string) -> Trino VARCHAR.
        kdbClient.execute("%s:([]id:1 2 3j; info:(%s;%s;%s))".formatted(
                tableName(table),
                qString("{\"name\":\"Alice\",\"age\":30, \"gender\":\"W\"}"),
                qString("{\"name\":\"Bob\",\"age\":25, \"gender\":\"M\"}"),
                qString("{\"name\":\"Carol\",\"age\":40, \"gender\":\"W\"}")));
    }

    @Override
    protected void createCoercionTable()
    {
        // enlist keeps "Alice" a single string (one row of VARCHAR) rather than a 5-row char column;
        // 42h is a q short so c_smallint maps to SMALLINT (exercises the SMALLINT->INTEGER coercion).
        kdbClient.execute(("%s:([]" +
                "id_col:1j; " +
                "c_varchar:enlist \"Alice\"; " +
                "c_smallint:42h)")
                .formatted(tableName(coercionTable)));
    }

    @Override
    protected SqlExecutor sourceSqlExecutor()
    {
        throw new UnsupportedOperationException("kdb+ connector is read-only");
    }

    @Override
    protected void dropSourceTable(CatalogSchemaTableName sourceTable)
    {
        if (kdbClient.tableExists(sourceTable.getSchemaTableName())) {
            // delete <name> from `. removes the global variable from the root namespace.
            kdbClient.execute("delete %s from `.".formatted(tableName(sourceTable)));
        }
    }

    @Test
    @Disabled("kdb+ is read-only: CREATE TABLE AS SELECT and ALTER TABLE ADD COLUMN are not supported on the source")
    @Override
    public void testSubstitutionAfterAddColumnToBaseTable() {}

    private static String tableName(CatalogSchemaTableName table)
    {
        return table.getSchemaTableName().getTableName();
    }

    private static String qString(String value)
    {
        return "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }
}
