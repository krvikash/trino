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

import com.google.common.base.Throwables;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import io.airlift.log.Logger;
import io.trino.testing.sql.SqlExecutor;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import static java.time.temporal.ChronoUnit.SECONDS;
import static java.util.Objects.requireNonNull;

public final class DatabricksSqlExecutor
        implements SqlExecutor
{
    private static final Logger log = Logger.get(DatabricksSqlExecutor.class);
    private static final RetryPolicy<Object> RETRY_POLICY = RetryPolicy.builder()
            .handleIf(DatabricksSqlExecutor::isTransientError)
            .onRetry(e -> log.warn(e.getLastException(), "Retrying SQL execution (attempt %d)", e.getAttemptCount()))
            .withMaxRetries(3)
            .withBackoff(1, 10, SECONDS)
            .build();

    private final String url;
    private final String user;
    private final String password;

    public DatabricksSqlExecutor(String url, String user, String password)
    {
        this.url = requireNonNull(url, "url is null");
        this.user = requireNonNull(user, "user is null");
        this.password = requireNonNull(password, "password is null");
    }

    /**
     * All SQL passed here must be idempotent (e.g. {@code CREATE ... IF NOT EXISTS}, {@code DROP ... IF EXISTS}):
     * transient failures retry the full body, which re-opens the connection and re-runs the statement.
     */
    @Override
    public void execute(String sql)
    {
        Failsafe.with(RETRY_POLICY).run(() -> executeOnce(sql));
    }

    private void executeOnce(String sql)
    {
        try (Connection connection = DriverManager.getConnection(url, user, password);
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static boolean isTransientError(Throwable e)
    {
        return Throwables.getCausalChain(e).stream().anyMatch(t -> {
            String message = t.getMessage();
            return message != null && (
                    message.contains("429") || message.contains("500") || message.contains("502") || message.contains("503") || message.contains("504"));
        });
    }
}
