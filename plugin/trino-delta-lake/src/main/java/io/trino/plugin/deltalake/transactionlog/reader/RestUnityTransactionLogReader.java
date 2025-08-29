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
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.deltalake.metastore.DeltaLakeTableOperationsProvider;
import io.trino.plugin.deltalake.transactionlog.Transaction;
import io.trino.plugin.deltalake.transactionlog.TransactionLogEntries;
import io.trino.plugin.deltalake.transactionlog.checkpoint.TransactionLogTail;
import io.trino.plugin.hive.metastore.unity.StagedCommit;
import io.trino.plugin.hive.metastore.unity.StagedCommitsInfo;
import io.trino.spi.connector.ConnectorSession;

import java.io.IOException;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogUtil.getTransactionLogStagedCommitDirectoryPath;
import static java.util.Objects.requireNonNull;

class RestUnityTransactionLogReader
        implements TransactionLogReader
{
    private final String tableId;
    private final String tableLocation;
    private final TrinoFileSystemFactory fileSystemFactory;
    private final FileSystemTransactionLogReader fileSystemTransactionLogReader;
    private final DeltaLakeTableOperationsProvider tableOperationsProvider;

    public RestUnityTransactionLogReader(
            String tableId,
            String tableLocation,
            TrinoFileSystemFactory fileSystemFactory,
            DeltaLakeTableOperationsProvider deltaLakeTableOperationsProvider)
    {
        this.tableId = requireNonNull(tableId, "tableId is null");
        this.tableLocation = requireNonNull(tableLocation, "tableLocation is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.fileSystemTransactionLogReader = new FileSystemTransactionLogReader(tableLocation, fileSystemFactory);
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
        TrinoFileSystem fileSystem = fileSystemFactory.create(session);

        // always load staged commits first, since there is a rare case that a staged commit is created and
        // just publishing when we are reading the transaction log from file system, in this case we may not
        // see the published commit from file system as it is still publishing, then we load staged commits
        // also not see the commit - as it just finished published after we load from file system.
        StagedCommitsInfo stagedCommitsInfo = tableOperationsProvider.createTableOperations(session)
                .loadStagedCommitsInfo(tableId, tableLocation, startVersion, endVersion);

        ImmutableList.Builder<Transaction> transactions = ImmutableList.builder();
        TransactionLogTail published = fileSystemTransactionLogReader.loadNewTail(session, startVersion, endVersion, transactionLogMaxCachedFileSize);
        transactions.addAll(published.getTransactions());
        long version = published.getVersion();
        if (endVersion.isPresent() && version == endVersion.get()) {
            return new TransactionLogTail(transactions.build(), version);
        }

        if (stagedCommitsInfo.getCommits() == null || stagedCommitsInfo.getCommits().isEmpty()) {
            // If there are no staged commits, the published latest version must be equal or greater than the current version.
            checkArgument(version >= stagedCommitsInfo.getLatestTableVersion(),
                    "Latest published version: %s is less than expected known version: %s", version, stagedCommitsInfo.getLatestTableVersion());
            // No new commits available, return the current transactions
            return new TransactionLogTail(transactions.build(), version);
        }

        long firstStagedCommitVersion = stagedCommitsInfo.getCommits().getFirst().version();
        checkArgument(version >= firstStagedCommitVersion - 1,
                "There is a gap between the published version: %s and the first staged commit: %s", version, firstStagedCommitVersion);

        if (stagedCommitsInfo.getLatestTableVersion() == version) {
            // If the latest table version is equal to the current version, it means no new commits are available.
            return new TransactionLogTail(transactions.build(), version);
        }

        for (StagedCommit commit : stagedCommitsInfo.getCommits()) {
            // During backfill, we first write the entry and then bump the unbackfill version.
            // This introduces a gap period where the version is not yet updated.
            if (commit.version() <= version) {
                continue;
            }
            Location transactionLogStagingEntryPath = getTransactionLogStagedCommitDirectoryPath(tableLocation).appendPath(commit.fileName());
            TransactionLogEntries transactionLogEntries = new TransactionLogEntries(commit.version(), fileSystem.newInputFile(transactionLogStagingEntryPath), transactionLogMaxCachedFileSize);
            transactions.add(new Transaction(commit.version(), transactionLogEntries));
            version = Math.max(version, commit.version());
        }
        return new TransactionLogTail(transactions.build(), version);
    }
}
