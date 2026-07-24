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
package io.trino.plugin.exasol.substitution;

import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.plugin.exasol.ExasolQueryRunner;
import io.trino.plugin.exasol.TestingExasolServer;
import io.trino.plugin.iceberg.substitution.AbstractIcebergMvSubstitutionTest;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.SqlExecutor;
import io.trino.tpch.TpchTable;
import org.junit.jupiter.api.parallel.Execution;

import static io.trino.plugin.exasol.TestingExasolServer.TEST_SCHEMA;
import static io.trino.plugin.hive.substitution.AbstractMvSubstitutionTest.SubFieldTestContext.VARCHAR_JSON_VALUES;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@Execution(SAME_THREAD)
public class TestIcebergMvOnExasolSourceSubstitution
        extends AbstractIcebergMvSubstitutionTest
{
    private TestingExasolServer exasolServer;

    @Override
    protected QueryRunner createSourceQueryRunner(Session defaultSession, CatalogSchemaName sourceSchema)
            throws Exception
    {
        exasolServer = closeAfterClass(new TestingExasolServer());
        return ExasolQueryRunner.builder(exasolServer)
                .setInitialTables(ImmutableSet.of(TpchTable.ORDERS, TpchTable.LINE_ITEM))
                .addExtraProperty("materialized-view-substitution.support.enabled", "true")
                .build();
    }

    // ExasolQueryRunner already provisions the exasol catalog, its tpch schema (loaded with the
    // initial tables above), and a tpch catalog. The source tables are created via the raw Exasol
    // JDBC executor, so no Trino-side source schema is created.
    @Override
    protected CatalogSchemaName sourceSchema()
    {
        return new CatalogSchemaName("exasol", TEST_SCHEMA);
    }

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
        exasolServer.execute(
                """
                CREATE TABLE %s AS SELECT
                    orderkey,
                    custkey,
                    CAST(orderdate AS CHAR(10)) AS orderdate,
                    totalprice,
                    CAST(orderstatus AS CHAR(1)) AS orderstatus
                    FROM tpch.orders where orderkey between 20000 and 20010""".formatted(sourceTableReference(ordersTable)));
    }

    @Override
    protected void createLineItemTable()
    {
        exasolServer.execute(
                """
                CREATE TABLE %s AS SELECT
                        orderkey,
                        linenumber,
                        quantity,
                        extendedprice,
                        CAST(shipdate AS CHAR(10)) AS shipdate
                        FROM tpch.lineitem where orderkey between 20000 and 20010""".formatted(sourceTableReference(lineitemTable)));
    }

    @Override
    protected void insertIntoOrders(long id)
    {
        exasolServer.execute("INSERT INTO %s VALUES (%s, 400, '1995-02-01', 99.99, 'N')".formatted(sourceTableReference(ordersTable), id));
    }

    @Override
    protected SqlExecutor sourceSqlExecutor()
    {
        return exasolServer::execute;
    }

    @Override
    protected String sourceTableReference(CatalogSchemaTableName sourceTable)
    {
        return sourceTable.getSchemaTableName().toString();
    }

    @Override
    protected SubFieldTestContext subFieldTestContext()
    {
        return new SubFieldTestContext("VARCHAR(1000)", VARCHAR_JSON_VALUES)
        {
            @Override
            protected String subFieldExpression(String column, String field)
            {
                return "JSON_VALUE(" + column + ", 'lax $." + field + "')";
            }
        };
    }
}
