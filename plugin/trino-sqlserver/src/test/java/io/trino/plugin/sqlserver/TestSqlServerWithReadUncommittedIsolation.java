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
package io.trino.plugin.sqlserver;

import io.trino.testing.sql.SqlExecutor;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static java.sql.Connection.TRANSACTION_READ_UNCOMMITTED;

public class TestSqlServerWithReadUncommittedIsolation
        extends BaseSqlServerTransactionIsolationTest
{
    @Override
    protected void configureDatabase(SqlExecutor executor, String databaseName) {}

    @Override
    protected Map<String, String> connectorProperties()
    {
        return Map.of("sqlserver.transaction-isolation-level", "READ_UNCOMMITTED");
    }

    @Test
    public void testSessionUsesReadUncommittedIsolation()
    {
        assertQuery(
                "SELECT * FROM TABLE(system.query(query => " +
                        "'SELECT CAST(transaction_isolation_level AS INT) AS isolation_level FROM sys.dm_exec_sessions WHERE session_id = @@SPID'))",
                "VALUES %d".formatted(TRANSACTION_READ_UNCOMMITTED));
    }
}
