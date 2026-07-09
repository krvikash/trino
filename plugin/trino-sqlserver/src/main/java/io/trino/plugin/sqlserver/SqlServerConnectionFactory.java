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

import com.google.common.cache.CacheBuilder;
import io.trino.cache.NonEvictableCache;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.sqlserver.SqlServerConfig.TransactionIsolationLevel;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static com.microsoft.sqlserver.jdbc.ISQLServerConnection.TRANSACTION_SNAPSHOT;
import static io.trino.cache.SafeCaches.buildNonEvictableCache;
import static io.trino.plugin.sqlserver.SqlServerConfig.TransactionIsolationLevel.SNAPSHOT;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.sql.Connection.TRANSACTION_READ_COMMITTED;
import static java.sql.Connection.TRANSACTION_READ_UNCOMMITTED;
import static java.time.Duration.ofMinutes;
import static java.util.Objects.requireNonNull;

public class SqlServerConnectionFactory
        implements ConnectionFactory
{
    private final NonEvictableCache<SnapshotIsolationEnabledCacheKey, Boolean> snapshotIsolationEnabled = buildNonEvictableCache(
            CacheBuilder.newBuilder()
                    .maximumSize(1)
                    .expireAfterWrite(ofMinutes(5)));

    private final ConnectionFactory delegate;
    private final boolean snapshotIsolationDisabled;
    private final Optional<TransactionIsolationLevel> transactionIsolationLevel;

    public SqlServerConnectionFactory(ConnectionFactory delegate, boolean snapshotIsolationDisabled, Optional<TransactionIsolationLevel> transactionIsolationLevel)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.snapshotIsolationDisabled = snapshotIsolationDisabled;
        this.transactionIsolationLevel = requireNonNull(transactionIsolationLevel, "transactionIsolationLevel is null");
    }

    @Override
    public Connection openConnection(ConnectorSession session)
            throws SQLException
    {
        Connection connection = delegate.openConnection(session);
        try {
            prepare(connection);
        }
        catch (SQLException e) {
            try (Connection ignored = connection) {
                throw e;
            }
        }
        return connection;
    }

    private void prepare(Connection connection)
            throws SQLException
    {
        if (snapshotIsolationDisabled) {
            return;
        }
        try {
            // Default to SNAPSHOT, as SQL Server's READ COMMITTED + SNAPSHOT ISOLATION is equivalent to ordinary READ COMMITTED in e.g. Oracle, PostgreSQL.
            switch (transactionIsolationLevel.orElse(SNAPSHOT)) {
                case READ_COMMITTED -> connection.setTransactionIsolation(TRANSACTION_READ_COMMITTED);
                case READ_UNCOMMITTED -> connection.setTransactionIsolation(TRANSACTION_READ_UNCOMMITTED);
                case SNAPSHOT -> {
                    if (hasSnapshotIsolationEnabled(connection)) {
                        connection.setTransactionIsolation(TRANSACTION_SNAPSHOT);
                    }
                    else if (transactionIsolationLevel.isPresent()) {
                        throw new SQLException("SNAPSHOT transaction isolation level requires ALLOW_SNAPSHOT_ISOLATION on database '%s'. ".formatted(connection.getCatalog()));
                    }
                }
            }
        }
        catch (SQLException e) {
            connection.close();
            throw e;
        }
    }

    private boolean hasSnapshotIsolationEnabled(Connection connection)
            throws SQLException
    {
        try {
            return snapshotIsolationEnabled.get(SnapshotIsolationEnabledCacheKey.INSTANCE, () -> {
                Handle handle = Jdbi.open(connection);
                return handle.createQuery("SELECT snapshot_isolation_state FROM sys.databases WHERE name = :name")
                        .bind("name", connection.getCatalog())
                        .mapTo(Boolean.class)
                        .findOne()
                        .orElse(false);
            });
        }
        catch (ExecutionException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, e);
        }
    }

    @Override
    public void close()
            throws SQLException
    {
        delegate.close();
    }

    private enum SnapshotIsolationEnabledCacheKey
    {
        // The snapshot isolation can be enabled or disabled on database level. We connect to single
        // database, so from our perspective, this is a global property.
        INSTANCE
    }
}
