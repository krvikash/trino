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
package io.trino.plugin.deltalake.metastore.unity;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.deltalake.metastore.DeltaLakeTableOperations;
import io.trino.plugin.deltalake.transactionlog.MetadataEntry;
import io.trino.plugin.deltalake.transactionlog.ProtocolEntry;
import io.trino.plugin.deltalake.transactionlog.writer.TransactionConflictException;
import io.trino.plugin.hive.metastore.unity.UnityCatalogException;
import io.trino.plugin.hive.metastore.unity.UnityMetastore;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.SchemaTableName;
import io.unitycatalog.client.delta.model.DeltaAddCommitUpdate;
import io.unitycatalog.client.delta.model.DeltaCommit;
import io.unitycatalog.client.delta.model.DeltaLoadTableResponse;
import io.unitycatalog.client.delta.model.DeltaProtocol;
import io.unitycatalog.client.delta.model.DeltaSetLatestBackfilledVersionUpdate;
import io.unitycatalog.client.delta.model.DeltaSetPartitionColumnsUpdate;
import io.unitycatalog.client.delta.model.DeltaSetProtocolUpdate;
import io.unitycatalog.client.delta.model.DeltaSetSchemaUpdate;
import io.unitycatalog.client.delta.model.DeltaSetTableCommentUpdate;
import io.unitycatalog.client.delta.model.DeltaTableUpdate;

import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.deltalake.metastore.unity.DeltaLakeUnitySchemaConverter.toDeltaStructType;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_METASTORE_ERROR;
import static java.util.Comparator.comparing;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElse;

public class DeltaLakeUnityTableOperations
        implements DeltaLakeTableOperations
{
    private final UnityMetastore metastore;

    public DeltaLakeUnityTableOperations(UnityMetastore metastore)
    {
        this.metastore = requireNonNull(metastore, "metastore is null");
    }

    @Override
    public void commitToExistingTable(SchemaTableName schemaTableName, long version, String schemaString, Optional<String> tableComment)
    {
        throw new UnsupportedOperationException("Unity Catalog does not support committing to existing tables");
    }

    @Override
    public UnityStagedCommits loadStagedCommitsInfo(SchemaTableName schemaTableName, String tableId, Optional<Long> startVersion, Optional<Long> endVersion)
    {
        DeltaLoadTableResponse response = metastore.loadStagedCommits(schemaTableName, tableId);
        // the Delta API returns all unbackfilled commits in no guaranteed order, so apply the inclusive version range here
        long start = startVersion.orElse(0L);
        List<DeltaCommit> commits = requireNonNullElse(response.getCommits(), List.<DeltaCommit>of()).stream()
                .filter(commit -> commit.getVersion() >= start)
                .filter(commit -> endVersion.map(end -> commit.getVersion() <= end).orElse(true))
                .sorted(comparing(DeltaCommit::getVersion))
                .collect(toImmutableList());
        return new UnityStagedCommits(commits, response.getLatestTableVersion());
    }

    @Override
    public void commitStagedCommits(
            SchemaTableName schemaTableName,
            String tableId,
            Optional<DeltaCommit> stagedCommit,
            Optional<Long> lastKnownBackfilledVersion,
            Optional<MetadataEntry> metadataEntry,
            Optional<ProtocolEntry> protocolEntry)
    {
        ImmutableList.Builder<DeltaTableUpdate> updates = ImmutableList.builder();
        stagedCommit.ifPresent(commit -> updates.add(new DeltaAddCommitUpdate().commit(commit)));
        lastKnownBackfilledVersion.ifPresent(version -> updates.add(new DeltaSetLatestBackfilledVersionUpdate().latestPublishedVersion(version)));
        metadataEntry.ifPresent(entry -> {
            updates.add(new DeltaSetSchemaUpdate().columns(toDeltaStructType(entry.getSchemaString())));
            updates.add(new DeltaSetPartitionColumnsUpdate().partitionColumns(entry.getOriginalPartitionColumns()));
            if (entry.getDescription() != null) {
                updates.add(new DeltaSetTableCommentUpdate().comment(entry.getDescription()));
            }
        });
        protocolEntry.ifPresent(entry -> updates.add(new DeltaSetProtocolUpdate().protocol(new DeltaProtocol()
                .minReaderVersion(entry.minReaderVersion())
                .minWriterVersion(entry.minWriterVersion())
                .readerFeatures(entry.readerFeatures().map(ImmutableList::copyOf).orElse(ImmutableList.of()))
                .writerFeatures(entry.writerFeatures().map(ImmutableList::copyOf).orElse(ImmutableList.of())))));
        try {
            metastore.commitStagedCommits(schemaTableName, tableId, updates.build());
        }
        catch (UnityCatalogException e) {
            if (e.getStatusCode() == 409) {
                throw new TransactionConflictException(e.getMessage(), e);
            }
            throw new TrinoException(HIVE_METASTORE_ERROR, e.getMessage(), e);
        }
    }
}
