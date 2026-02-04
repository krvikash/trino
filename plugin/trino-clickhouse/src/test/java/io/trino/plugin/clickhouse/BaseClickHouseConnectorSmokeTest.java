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
package io.trino.plugin.clickhouse;

import io.trino.plugin.jdbc.BaseJdbcConnectorSmokeTest;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public abstract class BaseClickHouseConnectorSmokeTest
        extends BaseJdbcConnectorSmokeTest
{
    protected abstract TestingClickHouseServer getClickHouseServer();

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_UPDATE,
                 SUPPORTS_DELETE,
                 SUPPORTS_MERGE -> false;
            case SUPPORTS_TRUNCATE -> true;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Test
    @Override
    public void testShowCreateTable()
    {
        // Override to add table properties
        assertThat((String) computeScalar("SHOW CREATE TABLE region"))
                .isEqualTo("" +
                        "CREATE TABLE clickhouse.tpch.region (\n" +
                        "   regionkey bigint,\n" +
                        "   name varchar,\n" +
                        "   comment varchar\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   engine = 'LOG'\n" +
                        ")");
    }

    @Test
    public void testReadTupleColumn()
    {
        try (TestTable testTable = new TestTable(
                getClickHouseServer()::execute,
                "tpch.test_tuple_smoke",
                "(id Int32, data Tuple(name String, score Int32)) ENGINE=Log")) {
            getClickHouseServer().execute("INSERT INTO " + testTable.getName() + " VALUES (1, ('Alice', 95)), (2, ('Bob', 80))");
            assertThat(query("SELECT * FROM " + testTable.getName() + " ORDER BY id"))
                    .matches("VALUES " +
                            "(1, CAST(ROW(VARCHAR 'Alice', 95) AS ROW(name varchar, score integer))), " +
                            "(2, CAST(ROW(VARCHAR 'Bob', 80) AS ROW(name varchar, score integer)))");

            assertThat(query("SELECT data.name, data.score FROM " + testTable.getName() + " ORDER BY id"))
                    .matches("VALUES (VARCHAR 'Alice', 95), (VARCHAR 'Bob', 80)");
        }
    }

    @Test
    public void testReadArrayColumn()
    {
        try (TestTable testTable = new TestTable(
                getClickHouseServer()::execute,
                "tpch.test_array_smoke",
                "(id Int32, scores Array(Int32), tags Array(String)) ENGINE=Log")) {
            getClickHouseServer().execute("INSERT INTO " + testTable.getName() + " VALUES (1, [10, 20], ['a', 'b']), (2, [42], ['c'])");
            assertThat(query("SELECT * FROM " + testTable.getName() + " ORDER BY id"))
                    .matches("VALUES " +
                            "(1, ARRAY[10, 20], CAST(ARRAY['a', 'b'] AS array(varchar))), " +
                            "(2, ARRAY[42], CAST(ARRAY['c'] AS array(varchar)))");
        }
    }
}
