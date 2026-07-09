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

import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.SqlExecutor;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class TestSqlServerSnapshotIsolationLevelWithoutSnapshotSupport
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        TestingSqlServer sqlServer = closeAfterClass(new TestingSqlServer(this::configureDatabase));
        return SqlServerQueryRunner.builder(sqlServer)
                .addConnectorProperties(Map.of("sqlserver.transaction-isolation-level", "SNAPSHOT"))
                .build();
    }

    private void configureDatabase(SqlExecutor executor, String databaseName)
    {
        executor.execute("ALTER DATABASE %s SET ALLOW_SNAPSHOT_ISOLATION OFF".formatted(databaseName));
        executor.execute("ALTER DATABASE %s SET READ_COMMITTED_SNAPSHOT OFF".formatted(databaseName));
    }

    @Test
    public void testSnapshotIsolationLevelWithoutAllowSnapshotIsolationFails()
    {
        assertQueryFails(
                "SELECT * FROM TABLE(sqlserver.system.query(query => 'SELECT 1'))",
                ".*SNAPSHOT transaction isolation level requires ALLOW_SNAPSHOT_ISOLATION on database .*");
    }
}
