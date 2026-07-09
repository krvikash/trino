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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.LegacyConfig;
import jakarta.validation.constraints.AssertTrue;

import java.util.Optional;

public class SqlServerConfig
{
    public enum TransactionIsolationLevel
    {
        READ_UNCOMMITTED,
        READ_COMMITTED,
        SNAPSHOT,
    }

    private boolean snapshotIsolationDisabled;
    private Optional<TransactionIsolationLevel> transactionIsolationLevel = Optional.empty();
    private boolean bulkCopyForWrite;
    private boolean bulkCopyForWriteLockDestinationTable;
    private boolean storedProcedureTableFunctionEnabled;

    public boolean isBulkCopyForWrite()
    {
        return bulkCopyForWrite;
    }

    @Config("sqlserver.bulk-copy-for-write.enabled")
    @ConfigDescription("Use SQL Server Bulk Copy API for writes")
    public SqlServerConfig setBulkCopyForWrite(boolean bulkCopyForWrite)
    {
        this.bulkCopyForWrite = bulkCopyForWrite;
        return this;
    }

    public boolean isBulkCopyForWriteLockDestinationTable()
    {
        return bulkCopyForWriteLockDestinationTable;
    }

    @Config("sqlserver.bulk-copy-for-write.lock-destination-table")
    @ConfigDescription("Obtain a Bulk Update lock on destination table on write")
    public SqlServerConfig setBulkCopyForWriteLockDestinationTable(boolean bulkCopyForWriteLockDestinationTable)
    {
        this.bulkCopyForWriteLockDestinationTable = bulkCopyForWriteLockDestinationTable;
        return this;
    }

    @Deprecated
    public boolean isSnapshotIsolationDisabled()
    {
        return snapshotIsolationDisabled;
    }

    @Deprecated
    @Config("sqlserver.snapshot-isolation.disabled")
    @ConfigDescription("Disables automatic use of snapshot isolation for transactions issued by Trino in SQL Server")
    public SqlServerConfig setSnapshotIsolationDisabled(boolean snapshotIsolationDisabled)
    {
        this.snapshotIsolationDisabled = snapshotIsolationDisabled;
        return this;
    }

    public Optional<TransactionIsolationLevel> getTransactionIsolationLevel()
    {
        return transactionIsolationLevel;
    }

    @Config("sqlserver.transaction-isolation-level")
    @ConfigDescription("Transaction isolation level applied to each connection, overriding the default SNAPSHOT isolation level used by Trino")
    public SqlServerConfig setTransactionIsolationLevel(TransactionIsolationLevel transactionIsolationLevel)
    {
        this.transactionIsolationLevel = Optional.ofNullable(transactionIsolationLevel);
        return this;
    }

    public boolean isStoredProcedureTableFunctionEnabled()
    {
        return storedProcedureTableFunctionEnabled;
    }

    @Config("sqlserver.stored-procedure-table-function-enabled")
    @LegacyConfig("sqlserver.experimental.stored-procedure-table-function-enabled")
    @ConfigDescription("Allows accessing Stored procedure as a table function")
    public SqlServerConfig setStoredProcedureTableFunctionEnabled(boolean storedProcedureTableFunctionEnabled)
    {
        this.storedProcedureTableFunctionEnabled = storedProcedureTableFunctionEnabled;
        return this;
    }

    @AssertTrue(message = "Set only one of sqlserver.transaction-isolation-level or the legacy sqlserver.snapshot-isolation.disabled")
    public boolean isTransactionIsolationConfigConsistent()
    {
        return !(snapshotIsolationDisabled && transactionIsolationLevel.isPresent());
    }
}
