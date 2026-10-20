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
package io.trino.plugin.deltalake.transactionlog.reader;

import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.plugin.deltalake.DeltaLakeFileSystemFactory;
import io.trino.plugin.deltalake.DeltaLakeTableCredentials;
import io.trino.plugin.deltalake.metastore.DeltaLakeTableOperationsProvider;
import io.trino.plugin.deltalake.metastore.unity.UnityStagedCommits;
import io.trino.plugin.deltalake.transactionlog.Transaction;
import io.trino.plugin.deltalake.transactionlog.TransactionLogEntries;
import io.trino.plugin.deltalake.transactionlog.checkpoint.TransactionLogTail;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.unitycatalog.client.delta.model.DeltaCommit;

import java.io.IOException;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogUtil.getTransactionLogStagedCommitDirectoryPath;
import static java.util.Objects.requireNonNull;

class RestUnityTransactionLogReader
        implements TransactionLogReader
{
    private final SchemaTableName schemaTableName;
    private final String tableId;
    private final String tableLocation;
    private final Optional<DeltaLakeTableCredentials> tableCredentials;
    private final DeltaLakeFileSystemFactory fileSystemFactory;
    private final FileSystemTransactionLogReader fileSystemTransactionLogReader;
    private final DeltaLakeTableOperationsProvider tableOperationsProvider;

    public RestUnityTransactionLogReader(
            SchemaTableName schemaTableName,
            String tableId,
            String tableLocation,
            Optional<DeltaLakeTableCredentials> tableCredentials,
            DeltaLakeFileSystemFactory fileSystemFactory,
            DeltaLakeTableOperationsProvider deltaLakeTableOperationsProvider)
    {
        this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
        this.tableId = requireNonNull(tableId, "tableId is null");
        this.tableLocation = requireNonNull(tableLocation, "tableLocation is null");
        this.tableCredentials = requireNonNull(tableCredentials, "tableCredentials is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.fileSystemTransactionLogReader = new FileSystemTransactionLogReader(tableLocation, tableCredentials, fileSystemFactory);
        this.tableOperationsProvider = requireNonNull(deltaLakeTableOperationsProvider, "deltaLakeTableOperationsProvider is null");
    }

    @Override
    public TransactionLogTail loadNewTail(
            ConnectorSession session,
            Optional<Long> startVersion,
            Optional<Long> endVersion,
            DataSize transactionLogMaxCachedFileSize)
            throws IOException
    {
        TrinoFileSystem fileSystem = fileSystemFactory.create(session, tableCredentials);

        // always load staged commits first, since there is a rare case that a staged commit is created and
        // just publishing when we are reading the transaction log from file system, in this case we may not
        // see the published commit from file system as it is still publishing, then we load staged commits
        // also not see the commit - as it just finished published after we load from file system.
        UnityStagedCommits stagedCommits = tableOperationsProvider.createTableOperations(session)
                .loadStagedCommitsInfo(schemaTableName, tableId, startVersion, endVersion);

        ImmutableList.Builder<Transaction> transactions = ImmutableList.builder();
        TransactionLogTail published = fileSystemTransactionLogReader.loadNewTail(session, startVersion, endVersion, transactionLogMaxCachedFileSize);
        transactions.addAll(published.getTransactions());
        long version = published.getVersion();
        if (endVersion.isPresent() && version == endVersion.get()) {
            return new TransactionLogTail(transactions.build(), version);
        }

        if (stagedCommits.commits().isEmpty()) {
            // If there are no staged commits, the published latest version must be equal or greater than the current version.
            checkArgument(version >= stagedCommits.latestTableVersion(),
                    "Latest published version: %s is less than expected known version: %s",
                    version,
                    stagedCommits.latestTableVersion());
            // No new commits available, return the current transactions
            return new TransactionLogTail(transactions.build(), version);
        }

        long firstStagedCommitVersion = stagedCommits.commits().getFirst().getVersion();
        checkArgument(version >= firstStagedCommitVersion - 1,
                "There is a gap between the published version: %s and the first staged commit: %s",
                version,
                firstStagedCommitVersion);

        if (stagedCommits.latestTableVersion() == version) {
            // If the latest table version is equal to the current version, it means no new commits are available.
            return new TransactionLogTail(transactions.build(), version);
        }

        for (DeltaCommit commit : stagedCommits.commits()) {
            // During backfill, we first write the entry and then bump the unbackfill version.
            // This introduces a gap period where the version is not yet updated.
            if (commit.getVersion() <= version) {
                continue;
            }
            Location transactionLogStagingEntryPath = getTransactionLogStagedCommitDirectoryPath(tableLocation).appendPath(commit.getFileName());
            TransactionLogEntries transactionLogEntries = new TransactionLogEntries(commit.getVersion(), fileSystem.newInputFile(transactionLogStagingEntryPath), transactionLogMaxCachedFileSize);
            transactions.add(new Transaction(commit.getVersion(), transactionLogEntries));
            version = Math.max(version, commit.getVersion());
        }
        return new TransactionLogTail(transactions.build(), version);
    }
}
