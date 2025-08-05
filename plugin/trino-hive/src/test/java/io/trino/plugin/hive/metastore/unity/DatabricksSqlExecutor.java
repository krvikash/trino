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
package io.trino.plugin.hive.metastore.unity;

import io.trino.testing.sql.SqlExecutor;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import static java.util.Objects.requireNonNull;

public final class DatabricksSqlExecutor
        implements SqlExecutor
{
    private final String url;
    private final String user;
    private final String password;

    public DatabricksSqlExecutor(String url, String user, String password)
    {
        this.url = requireNonNull(url, "url is null");
        this.user = requireNonNull(user, "user is null");
        this.password = requireNonNull(password, "password is null");
    }

    @Override
    public void execute(String sql)
    {
        try (Connection connection = DriverManager.getConnection(url, user, password);
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
