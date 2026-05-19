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

import com.databricks.sdk.core.DatabricksError;
import com.google.common.collect.ImmutableList;
import io.trino.plugin.deltalake.metastore.DeltaLakeTableOperations;
import io.trino.plugin.deltalake.transactionlog.MetadataEntry;
import io.trino.plugin.deltalake.transactionlog.ProtocolEntry;
import io.trino.plugin.deltalake.transactionlog.writer.TransactionConflictException;
import io.trino.plugin.hive.metastore.unity.CommitRequest;
import io.trino.plugin.hive.metastore.unity.Metadata;
import io.trino.plugin.hive.metastore.unity.Protocol;
import io.trino.plugin.hive.metastore.unity.StagedCommit;
import io.trino.plugin.hive.metastore.unity.StagedCommitsInfo;
import io.trino.plugin.hive.metastore.unity.UnityMetastore;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.SchemaTableName;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Optional;

import static io.trino.plugin.hive.HiveErrorCode.HIVE_METASTORE_ERROR;
import static java.util.Objects.requireNonNull;

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
    public StagedCommitsInfo loadStagedCommitsInfo(String tableId, String tableLocation, Optional<Long> startVersion, Optional<Long> endVersion)
    {
        return metastore.loadStagedCommitsInfo(tableId, tableLocation, startVersion, endVersion);
    }

    @Override
    public void commitStagedCommits(
            String tableId,
            String tableLocation,
            Optional<StagedCommit> stagedCommit,
            Optional<Long> lastKnownBackfilledVersion,
            Optional<MetadataEntry> metadataEntry,
            Optional<ProtocolEntry> protocolEntry)
    {
        Optional<Metadata> metadata = metadataEntry.map(entry -> new Metadata(
                tableId,
                entry.getName(),
                entry.getDescription(),
                entry.getOriginalPartitionColumns(),
                Instant.ofEpochMilli(entry.getCreatedTime()).atOffset(ZoneOffset.UTC).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)));
        Optional<Protocol> protocol = protocolEntry.map(entry -> new Protocol(
                entry.minReaderVersion(),
                entry.minWriterVersion(),
                entry.readerFeatures().map(ImmutableList::copyOf).orElse(ImmutableList.of()),
                entry.writerFeatures().map(ImmutableList::copyOf).orElse(ImmutableList.of())));
        try {
            metastore.commitStagedCommits(
                    new CommitRequest(tableId, tableLocation, stagedCommit.orElse(null), lastKnownBackfilledVersion.orElse(null), metadata.orElse(null), protocol.orElse(null)));
        }
        catch (DatabricksError e) {
            handleDatabricksError(e);
        }
    }

    // refer the error handling from
    // https://github.com/delta-io/delta/blob/967005713969fcf9280d2da024c761733b7879b5/storage/src/main/java/io/delta/storage/commit/uccommitcoordinator/UCTokenBasedRestClient.java#L211-L234
    private static void handleDatabricksError(DatabricksError error)
    {
        int statusCode = error.getStatusCode();
        if (statusCode == 409 && "ALREADY_EXISTS".equals(error.getErrorCode())) {
            throw new TrinoException(HIVE_METASTORE_ERROR, error.getMessage());
        }

        if (statusCode == 400 || statusCode == 404 || statusCode == 429) {
            throw new TrinoException(HIVE_METASTORE_ERROR, error.getMessage());
        }

        // let caller retry
        throw new TransactionConflictException(error.getMessage(), error);
    }
}
