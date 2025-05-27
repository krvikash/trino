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

import com.google.inject.Inject;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.deltalake.DeltaLakeTableHandle;
import io.trino.plugin.deltalake.ForUnityBackfill;
import io.trino.plugin.deltalake.metastore.DeltaLakeTableOperationsProvider;
import io.trino.plugin.deltalake.transactionlog.MetadataEntry;
import io.trino.plugin.deltalake.transactionlog.ProtocolEntry;
import io.trino.spi.connector.ConnectorSession;

import java.util.concurrent.ExecutorService;

import static io.trino.plugin.deltalake.DeltaLakeMetadata.isCatalogOwnedTable;
import static java.util.Objects.requireNonNull;

public class UnityTransactionLogWriterFactory
        implements TransactionLogWriterFactory
{
    private final TrinoFileSystemFactory fileSystemFactory;
    private final DeltaLakeTableOperationsProvider tableOperationsProvider;
    private final TransactionLogSynchronizerManager synchronizerManager;
    private final ExecutorService backfillExecutor;

    @Inject
    public UnityTransactionLogWriterFactory(
            TrinoFileSystemFactory fileSystemFactory,
            DeltaLakeTableOperationsProvider tableOperationsProvider,
            TransactionLogSynchronizerManager synchronizerManager,
            @ForUnityBackfill ExecutorService backfillExecutor)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.tableOperationsProvider = requireNonNull(tableOperationsProvider, "tableOperationsProvider is null");
        this.synchronizerManager = requireNonNull(synchronizerManager, "synchronizerManager is null");
        this.backfillExecutor = requireNonNull(backfillExecutor, "backfillExecutor is null");
    }

    @Override
    public TransactionLogWriter createWriter(ConnectorSession session, DeltaLakeTableHandle tableHandle)
    {
        return createWriter(session, tableHandle.location(), tableHandle.getMetadataEntry(), tableHandle.getProtocolEntry());
    }

    @Override
    public TransactionLogWriter createWriter(ConnectorSession session, String tableLocation, MetadataEntry metadataEntry, ProtocolEntry protocolEntry)
    {
        TransactionLogSynchronizer synchronizer = synchronizerManager.getSynchronizer(tableLocation);
        if (isCatalogOwnedTable(protocolEntry)) {
            String tableId = metadataEntry.getTableId()
                    .orElseThrow(() -> new IllegalStateException("tableId is not present"));
            return new UnityTransactionLogWriter(
                    tableId,
                    tableLocation,
                    fileSystemFactory.create(session),
                    tableOperationsProvider.createTableOperations(session),
                    new FileSystemTransactionLogWriter(session, synchronizer, tableLocation),
                    backfillExecutor);
        }
        return new FileSystemTransactionLogWriter(session, synchronizer, tableLocation);
    }

    @Override
    public TransactionLogWriter createFileSystemWriter(ConnectorSession session, String tableLocation)
    {
        return new FileSystemTransactionLogWriter(session, synchronizerManager.getSynchronizer(tableLocation), tableLocation);
    }

    @Override
    public TransactionLogWriter newWriterWithoutTransactionIsolation(ConnectorSession session, String tableLocation)
    {
        return new FileSystemTransactionLogWriter(session, synchronizerManager.getNoIsolationSynchronizer(), tableLocation);
    }
}
