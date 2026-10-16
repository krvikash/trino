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
package io.trino.tests.product.iceberg;

import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.QueryResult;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tests.product.iceberg.IcebergRestOAuth2PassthroughEnvironment.SCHEMA;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ProductTest
@RequiresEnvironment(IcebergRestOAuth2PassthroughEnvironment.class)
@TestGroup.IcebergRestOAuth2Passthrough
class TestIcebergRestOAuth2Passthrough
{
    @Test
    void testShowSchemas(IcebergRestOAuth2PassthroughEnvironment env)
            throws SQLException
    {
        try (Connection connection = env.createTrinoConnectionWithAccessToken(env.fetchAccessToken())) {
            assertThat(query(connection, "SHOW SCHEMAS IN iceberg").column(1)).contains(SCHEMA);
        }
    }

    @Test
    void testCreateTableAsSelect(IcebergRestOAuth2PassthroughEnvironment env)
            throws SQLException
    {
        String tableName = "iceberg.%s.table_%s".formatted(SCHEMA, randomNameSuffix());
        try (Connection connection = env.createTrinoConnectionWithAccessToken(env.fetchAccessToken())) {
            execute(connection, "CREATE TABLE %s AS SELECT 123 x".formatted(tableName));
            try {
                assertThat(query(connection, "SELECT * FROM " + tableName).column(1)).containsExactly(123);
            }
            finally {
                execute(connection, "DROP TABLE " + tableName);
            }
        }
    }

    @Test
    void testQueryWithoutAccessTokenFails(IcebergRestOAuth2PassthroughEnvironment env)
            throws SQLException
    {
        try (Connection connection = env.createTrinoConnection()) {
            assertThatThrownBy(() -> query(connection, "SHOW SCHEMAS IN iceberg"))
                    .hasMessageContaining("Token pass-through authentication requires a valid token, but none has been found");
        }
    }

    private static QueryResult query(Connection connection, String sql)
            throws SQLException
    {
        try (Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)) {
            return QueryResult.forResultSet(resultSet);
        }
    }

    private static void execute(Connection connection, String sql)
            throws SQLException
    {
        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }
}
