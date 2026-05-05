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
package io.trino.plugin.deltalake.transactionlog.writer;

import com.google.common.base.Throwables;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import io.airlift.log.Logger;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.plugin.deltalake.metastore.DeltaLakeTableOperations;
import io.trino.plugin.deltalake.transactionlog.AddFileEntry;
import io.trino.plugin.deltalake.transactionlog.CdcEntry;
import io.trino.plugin.deltalake.transactionlog.CommitInfoEntry;
import io.trino.plugin.deltalake.transactionlog.DeltaLakeTransactionLogEntry;
import io.trino.plugin.deltalake.transactionlog.MetadataEntry;
import io.trino.plugin.deltalake.transactionlog.ProtocolEntry;
import io.trino.plugin.deltalake.transactionlog.RemoveFileEntry;
import io.trino.plugin.deltalake.transactionlog.TransactionLogUtil;
import io.trino.plugin.hive.metastore.unity.StagedCommit;
import io.trino.plugin.hive.metastore.unity.StagedCommitsInfo;
import io.trino.spi.TrinoException;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

import static io.trino.plugin.deltalake.DeltaLakeErrorCode.DELTA_LAKE_FILESYSTEM_ERROR;
import static io.trino.plugin.deltalake.DeltaLakeErrorCode.DELTA_LAKE_INVALID_TABLE;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogUtil.getTransactionLogDir;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogUtil.getTransactionLogJsonEntryPath;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogUtil.getTransactionLogStagedCommitDirectoryPath;
import static java.util.Objects.requireNonNull;
import static org.apache.parquet.Preconditions.checkState;

public class UnityTransactionLogWriter
        implements TransactionLogWriter
{
    public static final Logger LOG = Logger.get(UnityTransactionLogWriter.class);

    // retry on the unity side, separate it from log writing retry
    private static final RetryPolicy<Object> BACKFILL_RETRY_POLICY = RetryPolicy.builder()
            .handleIf(throwable -> Throwables.getCausalChain(throwable).stream().anyMatch(TransactionConflictException.class::isInstance))
            .withDelay(Duration.ofMillis(400))
            .withJitter(Duration.ofMillis(200))
            .withMaxRetries(10)
            .onRetry(event -> LOG.debug(event.getLastException(), "Commit failed on attempt %d, will retry.", event.getAttemptCount()))
            .build();

    private final String tableId;
    private final String tableLocation;
    private final TrinoFileSystem fileSystem;
    private final DeltaLakeTableOperations tableOperations;
    private final FileSystemTransactionLogWriter fileSystemTransactionLogWriter;
    private final ExecutorService backfillExecutor;

    private Optional<DeltaLakeTransactionLogEntry> commitInfoEntry = Optional.empty();
    private Optional<DeltaLakeTransactionLogEntry> metadataEntry = Optional.empty();
    private Optional<DeltaLakeTransactionLogEntry> protocolEntry = Optional.empty();

    public UnityTransactionLogWriter(
            String tableId,
            String tableLocation,
            TrinoFileSystem fileSystem,
            DeltaLakeTableOperations tableOperations,
            FileSystemTransactionLogWriter fileSystemTransactionLogWriter,
            ExecutorService backfillExecutor)
    {
        this.tableId = requireNonNull(tableId, "tableId is null");
        this.tableLocation = requireNonNull(tableLocation, "tableLocation is null");
        this.fileSystem = requireNonNull(fileSystem, "fileSystem is null");
        this.tableOperations = requireNonNull(tableOperations, "tableOperations is null");
        this.fileSystemTransactionLogWriter = requireNonNull(fileSystemTransactionLogWriter, "fileSystemTransactionLogWriter is null");
        this.backfillExecutor = requireNonNull(backfillExecutor, "backfillExecutor is null");
    }

    @Override
    public void appendCommitInfoEntry(CommitInfoEntry commitInfoEntry)
    {
        checkState(this.commitInfoEntry.isEmpty(), "commitInfo already set");
        this.commitInfoEntry = Optional.of(DeltaLakeTransactionLogEntry.commitInfoEntry(commitInfoEntry));
        fileSystemTransactionLogWriter.appendCommitInfoEntry(commitInfoEntry);
    }

    @Override
    public void appendMetadataEntry(MetadataEntry metadataEntry)
    {
        checkState(this.metadataEntry.isEmpty(), "metadataEntry already set");
        this.metadataEntry = Optional.of(DeltaLakeTransactionLogEntry.metadataEntry(metadataEntry));
        fileSystemTransactionLogWriter.appendMetadataEntry(metadataEntry);
    }

    @Override
    public void appendProtocolEntry(ProtocolEntry protocolEntry)
    {
        checkState(this.protocolEntry.isEmpty(), "protocolEntry already set");
        this.protocolEntry = Optional.of(DeltaLakeTransactionLogEntry.protocolEntry(protocolEntry));
        fileSystemTransactionLogWriter.appendProtocolEntry(protocolEntry);
    }

    @Override
    public void appendAddFileEntry(AddFileEntry addFileEntry)
    {
        fileSystemTransactionLogWriter.appendAddFileEntry(addFileEntry);
    }

    @Override
    public void appendRemoveFileEntry(RemoveFileEntry removeFileEntry)
    {
        fileSystemTransactionLogWriter.appendRemoveFileEntry(removeFileEntry);
    }

    @Override
    public void appendCdcEntry(CdcEntry cdcEntry)
    {
        fileSystemTransactionLogWriter.appendCdcEntry(cdcEntry);
    }

    @Override
    public boolean isUnsafe()
    {
        return fileSystemTransactionLogWriter.isUnsafe();
    }

    @Override
    public void flush()
            throws IOException
    {
        checkState(commitInfoEntry.isPresent(), "commitInfo not set");

        CommitInfoEntry commitInfo = requireNonNull(commitInfoEntry.get().getCommitInfo(), "commitInfoEntry.get().getCommitInfo() is null");

        Location logEntry = TransactionLogUtil.getTransactionLogStagedCommitEntryPath(tableLocation, commitInfo.version());
        fileSystemTransactionLogWriter.writeLog(logEntry);
        TrinoInputFile logFile = fileSystem.newInputFile(logEntry);

        StagedCommit commit = new StagedCommit(
                commitInfo.version(),
                commitInfo.inCommitTimestamp().orElseThrow(),
                logEntry.fileName(),
                logFile.length(),
                logFile.lastModified().toEpochMilli());
        tableOperations.commitStagedCommits(
                tableId,
                tableLocation,
                Optional.of(commit),
                Optional.empty(),
                metadataEntry.map(DeltaLakeTransactionLogEntry::getMetaData),
                protocolEntry.map(DeltaLakeTransactionLogEntry::getProtocol));

        backfillExecutor.submit(() -> backfillToVersion(tableId, tableLocation, commitInfo.version()));
    }

    private void backfillToVersion(String tableId, String tableLocation, long version)
    {
        try {
            doBackfill(tableId, tableLocation, version);
        }
        catch (Throwable e) {
            LOG.warn(e, "Failed to backfill table %s to version %s".formatted(tableId, version));
        }
    }

    private void doBackfill(String tableId, String tableLocation, long version)
            throws IOException
    {
        StagedCommitsInfo stagedCommitsInfo = tableOperations.loadStagedCommitsInfo(tableId, tableLocation, Optional.empty(), Optional.of(version));
        if (stagedCommitsInfo.getCommits() == null || stagedCommitsInfo.getCommits().isEmpty()) {
            return;
        }

        List<StagedCommit> commits = stagedCommitsInfo.getCommits();
        long backfilledVersion = commits.getFirst().version() - 1;
        if (backfilledVersion >= 0 && !fileSystem.newInputFile(getTransactionLogJsonEntryPath(getTransactionLogDir(tableLocation), backfilledVersion)).exists()) {
            throw new TrinoException(DELTA_LAKE_INVALID_TABLE, "Backfilled commit entry file does not exist: " + getTransactionLogJsonEntryPath(getTransactionLogDir(tableLocation), backfilledVersion));
        }

        Location stagingPath = getTransactionLogStagedCommitDirectoryPath(tableLocation);
        for (StagedCommit commit : commits) {
            Location sourceLogPath = stagingPath.appendPath(commit.fileName());
            TrinoInputFile sourceLogFile = fileSystem.newInputFile(sourceLogPath);
            if (!sourceLogFile.exists()) {
                throw new TrinoException(DELTA_LAKE_INVALID_TABLE, "Commit entry file does not exist: " + sourceLogFile.location());
            }

            try (TrinoInput input = sourceLogFile.newInput()) {
                Location targetPath = getTransactionLogJsonEntryPath(getTransactionLogDir(tableLocation), commit.version());
                byte[] contents = input.readFully(0L, (int) sourceLogFile.length()).getBytes();

                try {
                    fileSystem.newOutputFile(targetPath).createExclusive(contents);
                }
                catch (FileAlreadyExistsException e) {
                    LOG.warn("Commit entry file already exists: %s, skipping backfill for version %d", targetPath, commit.version());
                    continue;
                }
                catch (IOException e) {
                    throw new TrinoException(DELTA_LAKE_FILESYSTEM_ERROR, "Fail to write commit file: " + targetPath);
                }
            }
            catch (IOException e) {
                throw new TrinoException(DELTA_LAKE_FILESYSTEM_ERROR, "Fail to read commit file: " + sourceLogFile);
            }

            Failsafe.with(BACKFILL_RETRY_POLICY).get(
                    _ -> {
                        tableOperations.commitStagedCommits(
                                tableId,
                                tableLocation,
                                Optional.empty(),
                                Optional.of(commit.version()),
                                Optional.empty(),
                                Optional.empty());
                        return null;
                    });
            // TODO: Add remove once we support the log retention policy
        }
    }
}
